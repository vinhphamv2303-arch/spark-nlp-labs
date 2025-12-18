# Báo cáo Thực hành: Pipeline tiền xử lý và mã hóa văn bản bằng vector thưa sử dụng Spark

---

## 1. Các bước triển khai

Quy trình được xây dựng theo mô hình **Pipeline của Spark MLlib**, chia thành **5 giai đoạn phát triển** (tương ứng với 5 bài lab):

### 1.1. Ingestion & Basic Preprocessing (Đọc & Tiền xử lý)

* Đọc dữ liệu JSON (`c4-train...`), giới hạn số lượng dòng thông qua biến `limitDocuments` để phục vụ kiểm thử nhanh.
* **Tokenization:** Sử dụng `RegexTokenizer` để tách từ dựa trên mẫu regex (thường là `\\w+`) thay vì chỉ tách theo khoảng trắng, giúp xử lý tốt dấu câu.
* **StopWords Removal:** Loại bỏ các từ vô nghĩa (stopwords) tiếng Anh phổ biến bằng `StopWordsRemover`.

---

### 1.2. Feature Engineering (Trích xuất đặc trưng – Bài 1 & 2)

* **TF-IDF:**

    * Sử dụng `HashingTF` để chuyển văn bản thành vector tần suất từ (Raw Features).
    * Áp dụng `IDF` để giảm trọng số các từ xuất hiện quá thường xuyên.
    * Kết quả thu được là **Sparse Vector** có chiều lớn.

* **Word2Vec (Nâng cao):**

    * Triển khai `Word2Vec` để học biểu diễn ngữ nghĩa của từ.
    * Vector đầu ra là **Dense Vector** với kích thước cố định (100 chiều).

---

### 1.3. Modeling (Mô hình hóa – Bài 3 & 4)

* Tích hợp mô hình **LogisticRegression** vào cuối pipeline.
* Huấn luyện mô hình cho bài toán **phân loại văn bản nhị phân** (Label: `0.0` hoặc `1.0`).
* So sánh hiệu quả mô hình khi đầu vào là:

    * TF-IDF (Bài 3)
    * Word2Vec (Bài 4)

---

### 1.4. Optimization & Search (Tối ưu & Tìm kiếm – Bài 5)

* **Normalization:**

    * Sử dụng `Normalizer` (chuẩn L2) để đưa vector về độ dài đơn vị.

* **Similarity Search:**

    * Xây dựng chức năng tìm kiếm văn bản tương đồng dựa trên **Cosine Similarity**.
    * So sánh vector truy vấn với toàn bộ vector trong tập dữ liệu.

---

### 1.5. Logging & Reporting

* Ghi log chi tiết thời gian thực thi của từng giai đoạn pipeline.
* Xuất kết quả dự đoán và kết quả tìm kiếm ra file text với định dạng rõ ràng để phân tích.

---

## 2. Hướng dẫn chạy code và ghi log

### 2.1. Môi trường

* jdk 11
* Scala 2.13
* Apache Spark 4.0.1

### 2.2. Cách chạy

Tại thư mục gốc của dự án, chạy lệnh:

```bash
sbt run
```

### 2.3. Cấu trúc Log và Output

* **Output:** `results/lab17_pipeline_output_1.txt`, `results/lab17_pipeline_output_2.txt`,...
* **Cosine similarity:** `results/lab17_final_output.txt`
* **System Log:** Thư mục `log/` (chứa metric hiệu năng và thời gian xử lý)

---

## 3. Giải thích kết quả thu được

### 3.1. Sử dụng Tokenizer thường (tách từ theo khoảng trắng), không dùng regex (Bài 1)
* **File:** `results/lab17_pipeline_output_1.txt`

* **Kết quả:** 
  
    * Số lượng token có thể ít hơn, một số dấu câu không được tách riêng, dẫn đến vector TF-IDF có thể khác biệt so với pipeline dùng RegexTokenizer.

### 3.2 Sử dụng RegexTokenizer và giảm kích thước vector hóa xuống 1000 chiều (Bài 2)
* **File:** `results/lab17_pipeline_output_2.txt`

* **Kết quả:**
  
    * Xuất hiện hiện tượng hash collision: nhiều từ khác nhau bị ánh xạ vào cùng một chiều, làm giảm độ phân biệt của vector.
### 3.3. Phân loại văn bản với Logistic Regression (Bài 3)

* **File:** `results/lab17_pipeline_output_3.txt`

* **Kết quả:**

    * Logistic Regression đạt độ chính xác cao trên tập mẫu (~20 dòng đầu).
    * Các văn bản mô tả sản phẩm (ví dụ: *"Foil plaid lycra..."*) được phân loại chính xác là lớp `0.0`.
    * Tin tức/hội thoại được phân loại là lớp `1.0`.

* **Giải thích:**

    * TF-IDF tạo ra vector thưa có khả năng bắt từ khóa rất tốt.
    * Với tập dữ liệu nhỏ, phương pháp dựa trên tần suất từ hoạt động hiệu quả hơn các mô hình học ngữ nghĩa phức tạp.

---

### 3.2. Phân loại văn bản với Word2Vec (Bài 4)

* **File:** `results/lab17_pipeline_output_4.txt`

* **Kết quả:**

    * Xuất hiện một số trường hợp phân loại sai.
    * Ví dụ: văn bản mô tả quần áo (*"Foil plaid lycra..."*) bị dự đoán nhầm thành tin tức (`1.0`).

* **Giải thích:**

    * Word2Vec tạo vector câu bằng cách **trung bình cộng vector các từ**.
    * Với câu ngắn, các từ quan trọng dễ bị “pha loãng”.
    * Tập dữ liệu huấn luyện nhỏ khiến Word2Vec không học được ngữ nghĩa đủ tốt.

---

### 3.3. Tìm kiếm tương đồng (Similarity Search – Bài 5)

* **File:** `results/lab17_pipeline_output_5.txt`

* **Truy vấn:** *"Beginners BBQ Class"*

* **Vector sử dụng:** Normalized TF-IDF (chuẩn L2)

* **Kết quả:**

    * Trả về các bài viết về Nhạc Jazz, Quả Bơ, Luật sư...
    * Điểm tương đồng thấp (~0.2).

* **Giải thích:**

    * Do **Data Sparsity**: tập dữ liệu mẫu không có bài viết cùng chủ đề BBQ.
    * Cosine Similarity buộc phải chọn các văn bản “ít khác biệt nhất” dựa trên các từ chung như *class*, *join*, *place*.
    * Dẫn đến hiện tượng **false positives**.

---

## 4. Khó khăn gặp phải và cách giải quyết

* Khi chạy dự án trên JDK 21, chương trình báo lỗi ngoại lệ java.lang.reflect.InaccessibleObjectException hoặc lỗi liên quan đến sun.misc.Unsafe trong quá trình khởi tạo Spark Context hoặc khi sử dụng thư viện Kryo Serialization.
* Giải quyết: Downgrade từ JDK 21 xuống JDK 11.

## 5. Tài liệu tham khảo và Mô hình sử dụng

### Tài liệu chính thức Apache Spark MLlib:

`https://spark.apache.org/docs/latest/ml-guide.html`

`https://spark.apache.org/docs/latest/ml-features.html`