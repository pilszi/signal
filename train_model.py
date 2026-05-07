import pandas as pd
import torch
from torch.utils.data import Dataset
from transformers import AutoTokenizer, AutoModelForSequenceClassification, Trainer, TrainingArguments
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import os


# 모델 학습
# 1. 환경 설정 및 데이터 로드
model_name = "klue/bert-base"
data_path = 'data/balanced_labeled_data.csv'

print("📂 학습할 데이터를 불러오는 중입니다...")
try:
    df = pd.read_csv(data_path, encoding='utf-8')
except UnicodeDecodeError:
    df = pd.read_csv(data_path, encoding='cp949')

# 라벨 매핑 (문자열 -> 숫자)
# label_map = {'neutral': 0, 'positive': 1, 'negative': 2}
# df['label'] = df['labels'].map(label_map)
df['label'] = df['label'] - 1
# 학습용과 검증용 데이터로 분리 (8:2 비율)
train_texts, val_texts, train_labels, val_labels = train_test_split(
    df['content'].tolist(),
    df['label'].tolist(),
    test_size=0.2,
    random_state=42
)

# 2. 토크나이저 준비
print("📝 토크나이저를 준비 중입니다...")
tokenizer = AutoTokenizer.from_pretrained(model_name)


# 3. 데이터셋 클래스 정의
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

# 텍스트 토큰화 진행
train_encodings = tokenizer(train_texts, truncation=True, padding=True, max_length=128)
val_encodings = tokenizer(val_texts, truncation=True, padding=True, max_length=128)

train_dataset = FinanceDataset(train_encodings, train_labels)
val_dataset = FinanceDataset(val_encodings, val_labels)

# --- 지표 계산 함수 추가 ---
def compute_metrics(pred):
    labels = pred.label_ids
    preds = pred.predictions.argmax(-1)
    acc = accuracy_score(labels, preds)
    return {'accuracy': acc}
# -----------------------

# 4. 모델 로드 및 학습 설정
print("🤖 모델을 빌드 중입니다...")
model = AutoModelForSequenceClassification.from_pretrained(model_name, num_labels=3)

training_args = TrainingArguments(
    output_dir='./results_2',          # 중간 결과 저장 폴더
    num_train_epochs=10,              # 전체 데이터 학습 횟수
    per_device_train_batch_size=16,  # 한 번에 학습할 데이터 양
    per_device_eval_batch_size=16,
    dataloader_num_workers=0,
    learning_rate=2e-5,               # 학습 속도를 조금 높여봅니다
    warmup_steps=500,                # 학습 초기 안정화 단계
    weight_decay=0.01,
    logging_dir='./logs',            # 로그 기록 폴더
    logging_steps=10,
    eval_strategy="epoch",     # 에포크마다 검증 수행
    save_strategy="epoch",
    load_best_model_at_end=True,     # 가장 성적이 좋았던 모델을 최종 선택
    metric_for_best_model="accuracy", # 정확도가 가장 높은 모델 저장
)

# 5. 트레이너 가동 (진짜 학습 시작)
print("🚀 학습을 시작합니다! (시간이 다소 소요될 수 있습니다)")
trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset,
    eval_dataset=val_dataset,
    compute_metrics=compute_metrics,
)

trainer.train()

# 6. 최종 모델 저장
save_path = "./fine_tuned_finance_model_2"
model.save_pretrained(save_path)
tokenizer.save_pretrained(save_path)

print(f"\n✅ 모든 과정이 완료되었습니다!")
print(f"📦 학습된 모델이 '{save_path}' 폴더에 저장되었습니다.")

