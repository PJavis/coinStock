# Import các thư viện cần thiết
from pyspark import SparkContext
from pyspark.sql import SparkSession

# Khởi tạo Spark context và session
sc = SparkContext("local", "Lambda Batch Layer")
spark = SparkSession(sc)

# Đường dẫn tới tập dữ liệu trên HDFS
data_path = "hdfs://your-hadoop-cluster/path/to/your/data.csv"

# Đọc dữ liệu từ tập tin CSV
raw_data = spark.read.csv(data_path, header=True, inferSchema=True)

# Xử lý dữ liệu theo lô (batch processing)
def process_batch_data(data):
    # Thực hiện các phép tính, lọc dữ liệu, tính toán, ...
    # Ví dụ: tính tổng theo cột "amount"
    result = data.groupBy("date").sum("amount")
    return result

# Gọi hàm xử lý dữ liệu theo lô
batch_result = process_batch_data(raw_data)

# Lưu kết quả vào cơ sở dữ liệu chỉ đọc (read-only database)
batch_result.write.mode("overwrite").parquet("hdfs://your-hadoop-cluster/path/to/batch_views")

# Đóng Spark session
spark.stop()

print("Batch processing completed and results saved.")
