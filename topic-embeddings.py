# prerequisites: transformers, bs4, json

from transformers import AutoTokenizer, AutoModel
from bs4 import BeautifulSoup
import json

tokenizer = AutoTokenizer.from_pretrained('allenai/specter')


# read topics set
with open('2020-07-16/topics-set.xml', 'r') as file:
    topics = file.read()
 
topics = BeautifulSoup(topics, 'xml')
topics = list(map(
    lambda x: x.find('query').contents[0] + tokenizer.sep_token + x.find('narrative').contents[0],
    topics.find_all('topic')
    )
)

# compute embeddings
model = AutoModel.from_pretrained('allenai/specter')
inputs = tokenizer(topics, padding=True, truncation=True, return_tensors="pt", max_length=768)
result = model(**inputs)
embeddings = result.last_hidden_state[:, 0, :].detach().numpy()


# save results in json
result = dict(zip(range(1, 51), map(lambda x: ' '.join(map(str, x)), embeddings)))

with open("2020-07-16/topics-embeddings.json", "w") as file:
    json.dump(result, file)