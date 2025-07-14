import pandas as pd

df = pd.read_csv("training.1600000.processed.noemoticon.csv", encoding="ISO-8859-1", header=None)
df.columns = ["target", "id", "date", "query", "user", "text"]

df["sentiment"] = df["target"].map({0: "negative", 2: "neutral", 4: "positive"})
df["timestamp"] = pd.to_datetime(df["date"], errors='coerce')

df_out = df[["id", "text", "sentiment", "timestamp"]]
df_out.to_parquet("sentiment140.parquet")
