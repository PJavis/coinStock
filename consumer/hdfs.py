import pyhdfs

# Tạo một đối tượng HdfsClient để kết nối tới HDFS
hdfs = pyhdfs.HdfsClient(hosts="localhost:9870", user_name="hdfs")

# Lấy thư mục home của người dùng trên HDFS
user_home_directory = hdfs.get_home_directory()
print("User's home directory:", user_home_directory)

# Lấy danh sách các tệp trong thư mục root của HDFS
root_files = hdfs.listdir("/")
print("Files in root directory:", root_files)
