import pandas as pd
import torch
from torch.utils.data import Dataset
from transformers import AutoTokenizer, AutoModelForSequenceClassification, Trainer, TrainingArguments, EarlyStoppingCallback
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_recall_fscore_support

# 1. 환경 설정 및 데이터 로드
model_name = "klue/bert-base"
data_path = 'balanced_labeled_data.csv'

print("📂 제목과 본문을 통합하여 데이터를 불러오는 중입니다...")
df = pd.read_csv(data_path)

# [핵심] 제목(title)과 본문(content)을 합친 새로운 컬럼 생성
# 제목이 비어있을 경우를 대비해 fillna('')를 처리해줍니다.
df['combined_text'] = df['title'].fillna('') + " [SEP] " + df['content'].fillna('')

# 라벨 매핑 (1, 2, 3 -> 0, 1, 2)
df['label'] = df['label'] - 1

# 학습용/검증용 분리 (combined_text 사용)
train_texts, val_texts, train_labels, val_labels = train_test_split(
    df['combined_text'].tolist(),
    df['label'].tolist(),
    test_size=0.2,
    random_state=42
)

# 2. 토크나이저 준비
tokenizer = AutoTokenizer.from_pretrained(model_name)

# 3. 데이터셋 클래스
class FinanceDataset(Dataset):
    def __init__(self, encodings, labels):
        self.encodings = encodings
        self.labels = labels
    def __getitem__(self, idx):
        item = {key: torch.tensor(val[idx]) for key, val in self.encodings.items()}
        item['label'] = torch.tensor(self.labels[idx])
        return item
    def __len__(self):
        return len(self.labels)

# 토큰화 (제목이 들어갔으므로 길이를 256으로 늘려주는 것이 안전합니다)
print("📝 통합 텍스트 토큰화 진행 중...")
train_encodings = tokenizer(train_texts, truncation=True, padding=True, max_length=256)
val_encodings = tokenizer(val_texts, truncation=True, padding=True, max_length=256)

train_dataset = FinanceDataset(train_encodings, train_labels)
val_dataset = FinanceDataset(val_encodings, val_labels)

# 4. 지표 계산 함수
def compute_metrics(pred):
    labels = pred.label_ids
    preds = pred.predictions.argmax(-1)
    precision, recall, f1, _ = precision_recall_fscore_support(labels, preds, average='macro')
    acc = accuracy_score(labels, preds)
    return {'accuracy': acc, 'f1': f1, 'precision': precision, 'recall': recall}

# 5. 모델 로드 및 학습 설정
print("🤖 제목 인지형 모델 빌드 중...")
model = AutoModelForSequenceClassification.from_pretrained(model_name, num_labels=3)

training_args = TrainingArguments(
    output_dir='./results_title_model', # 폴더명 구분
    num_train_epochs=7,
    per_device_train_batch_size=16,
    per_device_eval_batch_size=16,
    learning_rate=2e-5,
    warmup_steps=500,
    weight_decay=0.01,
    eval_strategy="epoch",
    save_strategy="epoch",
    load_best_model_at_end=True,
    metric_for_best_model="f1",
    report_to="none",
)

# 6. 트레이너 가동
trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset,
    eval_dataset=val_dataset,
    compute_metrics=compute_metrics,
    callbacks=[EarlyStoppingCallback(early_stopping_patience=2)]
)

print("🚀 학습 시작! (제목+본문)")
trainer.train()

# 7. 최종 모델 저장
save_path = "./fine_tuned_finance_model_with_title"
model.save_pretrained(save_path)
tokenizer.save_pretrained(save_path)

print(f"\n✅ 완료! 제목 통합 모델이 '{save_path}'에 저장되었습니다.")
import pandas as pd
import torch


# 모델이 분석에 어려움을 느끼는 기사 본문들을 csv 화하여 재라벨링

print("\n🔍 오답 분석을 위한 예측을 진행합니다...")

# 1. 검증 데이터셋에 대해 예측 수행
# trainer는 위에서 정의한 객체를 그대로 사용합니다.
output = trainer.predict(val_dataset)
predictions = output.predictions.argmax(-1) # 확률이 가장 높은 라벨 인덱스(0,1,2)

# 2. 결과 데이터프레임 생성
# val_texts에 들어있는 '[SEP]'를 기준으로 제목과 본문을 다시 나눕니다.
analysis_df = pd.DataFrame({
    'combined_text': val_texts,
    'actual_label': [l + 1 for l in val_labels],      # 0,1,2 -> 1,2,3단계로 복구
    'predicted_label': [p + 1 for p in predictions]   # 0,1,2 -> 1,2,3단계로 복구
})

# 3. 틀린 데이터만 필터링
# 실제 라벨과 예측 라벨이 다른 경우만 추출
errors = analysis_df[analysis_df['actual_label'] != analysis_df['predicted_label']].copy()

# 4. 보기 편하게 제목과 본문 분리 (선택 사항)
# [SEP]를 기준으로 쪼개서 가독성을 높입니다.
errors['title'] = errors['combined_text'].apply(lambda x: x.split(' [SEP] ')[0] if ' [SEP] ' in x else x)
errors['content'] = errors['combined_text'].apply(lambda x: x.split(' [SEP] ')[1] if ' [SEP] ' in x else "")

# 필요한 컬럼만 순서대로 정리
errors = errors[['title', 'content', 'actual_label', 'predicted_label']]

# 5. CSV 저장 및 다운로드
error_file_name = 'wrong_predictions.csv'
errors.to_csv(error_file_name, index=False, encoding='utf-8-sig')

print(f"✅ 총 {len(errors)}개의 오답 데이터를 찾았습니다.")
from google.colab import files
files.download(error_file_name)