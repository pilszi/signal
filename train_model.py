import pandas as pd
import torch
import numpy as np
import os
from torch.utils.data import Dataset
from transformers import AutoTokenizer, AutoModelForSequenceClassification, Trainer, TrainingArguments
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score


# 모델 학습
# 1. 환경 설정 및 경로
model_name = "klue/bert-base"
data_path_old = 'data/finance_data_balanced.csv'
data_path_new = 'data/balanced_labeled_data.csv'
save_path = "./fine_tuned_finance_model_3"

# --- [Step 1] 기존 데이터 로드 (finance_data_balanced.csv) ---
print("📂 기존 데이터를 로드 중...")
try:
    try:
        df_old = pd.read_csv(data_path_old, encoding='utf-8')
    except UnicodeDecodeError:
        df_old = pd.read_csv(data_path_old, encoding='cp949')

    # 🔥 [핵심] 모든 컬럼 이름의 앞뒤 공백을 싹 제거 (' labels' -> 'labels')
    df_old.columns = df_old.columns.str.strip()
    print(f"   🔎 기존 파일 컬럼명(정제후): {df_old.columns.tolist()}")

    # 라벨 매핑: 문자열 -> 숫자 (0:중립, 1:긍정, 2:부정)
    old_label_map = {'neutral': 0, 'positive': 1, 'negative': 2}
    df_old['label'] = df_old['labels'].map(old_label_map)
    df_old = df_old[['sentence', 'label']]

    print(f"✅ 기존 데이터 로드 완료: {len(df_old)}건")
except Exception as e:
    print(f"⚠️ 기존 데이터 로드 중 예상치 못한 오류: {e}")
    df_old = pd.DataFrame(columns=['sentence', 'label']) # 에러 시 빈 틀 유지


# --- [Step 2] 신규 데이터 로드 (balanced_labeled_data.csv) ---
print("📂 신규 데이터를 로드 중...")
try:
    try:
        df_new = pd.read_csv(data_path_new, encoding='utf-8')
    except UnicodeDecodeError:
        df_new = pd.read_csv(data_path_new, encoding='cp949')

    # 여기도 혹시 모르니 공백 제거!
    df_new.columns = df_new.columns.str.strip()
    print(f"   🔎 신규 파일 컬럼명(정제후): {df_new.columns.tolist()}")

    # 제목과 본문을 합쳐서 학습 문장 생성
    df_new['sentence'] = df_new['title'].fillna("") + " " + df_new['content'].fillna("")

    # 라벨 재매핑: (기존 1:안정/Pos, 2:주의/Neu, 3:심각/Neg) -> (변경 1:긍정, 0:중립, 2:부정)
    new_label_map = {1: 1, 2: 0, 3: 2}
    df_new['label'] = df_new['labels'].map(new_label_map)

    df_new = df_new[['sentence', 'label']]
    print(f"✅ 신규 데이터 로드 완료: {len(df_new)}건")

except Exception as e:
    print(f"⚠️ 신규 데이터 로드 중 예상치 못한 오류: {e}")
    df_new = pd.DataFrame(columns=['sentence', 'label'])


# --- [Step 3] 데이터 병합 및 최종 정제 ---
df = pd.concat([df_old, df_new], axis=0).reset_index(drop=True)
df = df.dropna(subset=['sentence', 'label'])
df['label'] = df['label'].astype(int)

print(f"📊 최종 통합 데이터 수: {len(df)}건")
print(df['label'].value_counts()) # 라벨별 분포 확인

# 학습용/검증용 분리
train_texts, val_texts, train_labels, val_labels = train_test_split(
    df['sentence'].tolist(),
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
        item['labels'] = torch.tensor(self.labels[idx])
        return item

    def __len__(self):
        return len(self.labels)

# 텍스트 토큰화 진행
train_encodings = tokenizer(train_texts, truncation=True, padding=True, max_length=128)
val_encodings = tokenizer(val_texts, truncation=True, padding=True, max_length=128)

train_dataset = FinanceDataset(train_encodings, train_labels)
val_dataset = FinanceDataset(val_encodings, val_labels)


# 3. 평가 지표 및 모델 설정
def compute_metrics(pred):
    labels = pred.label_ids
    preds = pred.predictions.argmax(-1)
    acc = accuracy_score(labels, preds)
    f1 = f1_score(labels, preds, average='weighted')
    return {'accuracy': acc, 'f1': f1}


# 4. 모델 로드 및 학습 설정
print("🤖 모델을 빌드 중입니다...")
device = "cuda" if torch.cuda.is_available() else "cpu"
model = AutoModelForSequenceClassification.from_pretrained(model_name, num_labels=3)
model.to(device)

training_args = TrainingArguments(
    output_dir='./results_3',          # 중간 결과 저장 폴더
    num_train_epochs=3,              # 전체 데이터 학습 횟수
    per_device_train_batch_size=16,  # 한 번에 학습할 데이터 양
    per_device_eval_batch_size=16,
    warmup_steps=100,                # 학습 초기 안정화 단계
    weight_decay=0.01,
    logging_dir='./logs',            # 로그 기록 폴더
    logging_steps=10,
    eval_strategy="epoch",     # 에포크마다 검증 수행
    save_strategy="epoch",
    load_best_model_at_end=True,     # 가장 성적이 좋았던 모델을 최종 선택
)

# 5. 트레이너 가동 (진짜 학습 시작)
print(f"🚀 학습 시작! (사용 장치: {device.upper()})")
trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset,
    eval_dataset=val_dataset,
    compute_metrics=compute_metrics # 성능 기록
)

trainer.train()

# 6. 최종 모델 저장
model.save_pretrained(save_path)
tokenizer.save_pretrained(save_path)
print(f"\n✅ 모든 과정이 완료되었습니다!")
print(f"📦 학습된 모델이 '{save_path}' 폴더에 저장되었습니다.")