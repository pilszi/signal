import pandas as pd
import torch
from torch.utils.data import Dataset, DataLoader
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from torch.optim import AdamW
from sklearn.model_selection import train_test_split
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

logging.getLogger("elasticsearch").setLevel(logging.WARNING)  # ES 내부 로그 숨기기
logging.getLogger("elastic_transport").setLevel(logging.WARNING) # 통신 로그 숨기기
logging.getLogger("urllib3").setLevel(logging.WARNING) # 네트워크 요청 로그 숨기기

# 1. 데이터 준비
class EconomicDataset(Dataset):
    def __init__(self, sentences, labels, tokenizer):
        self.tokenizer = tokenizer
        self.sentences = sentences
        # labels: negative=0, neutral=1, positive=2
        self.labels = labels

    def __len__(self):
        return len(self.sentences)

    def __getitem__(self, i):
        inputs = self.tokenizer(self.sentences[i], return_tensors="pt",
                                padding='max_length', truncation=True, max_length=128)
        return {
            'input_ids': inputs['input_ids'].flatten(),
            'attention_mask': inputs['attention_mask'].flatten(),
            'labels': torch.tensor(self.labels[i])
        }


def train_model(csv_path):
    # CSV 로드 및 라벨 변환
    df = pd.read_csv(csv_path)
    label_map = {'negative': 0, 'neutral': 1, 'positive': 2}
    df['label_num'] = df['labels'].map(label_map)

    # 학습용/검증용 데이터 분리
    train_texts, val_texts, train_labels, val_labels = train_test_split(
        df['kor_sentence'].values, df['label_num'].values, test_size=0.2
    )

    tokenizer = AutoTokenizer.from_pretrained("klue/bert-base")
    model = AutoModelForSequenceClassification.from_pretrained("klue/bert-base", num_labels=3)

    train_dataset = EconomicDataset(train_texts, train_labels, tokenizer)
    train_loader = DataLoader(train_dataset, batch_size=16, shuffle=True)

    # 학습 설정
    optimizer = AdamW(model.parameters(), lr=2e-5)
    model.train()

    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model.to(device)

    logging.info("🚀 학습 시작...")
    for epoch in range(3):  # 3번 반복 공부
        for batch in train_loader:
            optimizer.zero_grad()
            input_ids = batch['input_ids'].to(device)
            attention_mask = batch['attention_mask'].to(device)
            labels = batch['labels'].to(device)

            outputs = model(input_ids, attention_mask=attention_mask, labels=labels)
            loss = outputs.loss
            loss.backward()
            optimizer.step()
        logging.info(f"Epoch {epoch + 1} 완료 (Loss: {loss.item():.4f})")

    # 저장
    model.save_pretrained("trained_bert_model")
    tokenizer.save_pretrained("trained_bert_model")
    logging.info("✅ 학습 완료 및 모델 저장됨: ./trained_bert_model")


if __name__ == "__main__":
    train_model('./finance_data.csv')  # CSV 파일명을 넣어주세요