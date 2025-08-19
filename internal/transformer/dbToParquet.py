import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Load Tab separated file
df = pd.read_csv("../data/test.tsv", sep=",")

# Transform into arrow format
table = pa.Table.from_pandas(df)

# Save to parquet
pq.write_table(table, "/Users/nils/Documents/UZH/FS25/bsc/Go-Flight-Server/third_party/dataset/test.parquet")