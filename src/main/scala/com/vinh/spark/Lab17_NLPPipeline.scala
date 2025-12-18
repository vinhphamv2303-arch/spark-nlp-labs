package com.vinh.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{HashingTF, IDF, Normalizer, RegexTokenizer, StopWordsRemover, Tokenizer}
import org.apache.spark.sql.functions._
import java.io.{File, PrintWriter}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.Vector
// Import thêm thư viện Word2Vec
import org.apache.spark.ml.feature.Word2Vec

object Lab17_NLPPipeline {
  def main(args: Array[String]): Unit = {

    // --- [REQ 1] Customize Document Limit ---
    // Biến để dễ dàng thay đổi số lượng tài liệu cần xử lý
    val limitDocuments = 2000
    val numFeatures = 1000    // Số lượng features cho HashingTF

    val spark = SparkSession.builder
      .appName("Lab 17: Advanced NLP Pipeline - Search Engine")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    println(s"--- Starting Lab 17 with Document Limit: $limitDocuments ---")

    // --- [REQ 2] Measure Read Time ---
    val t0_read = System.nanoTime()

    // 1. --- Read Dataset ---
    val dataPath = "D:\\ADMIN\\Documents\\Classwork\\spark_labs\\data\\c4-train.00000-of-01024-30K.json.gz"
    val rawDF = spark.read.json(dataPath).limit(limitDocuments)

    // --- Create Fake Labels ---
    println("Generating fake labels for training...")
    val trainingDF = rawDF.withColumn("label", when(length($"text") > 200, 1.0).otherwise(0.0))

    // Cache data để đo thời gian đọc chính xác (Action count kích hoạt việc đọc)
    trainingDF.cache()
    val recordCount = trainingDF.count()

    val t1_read = System.nanoTime()
    println(f"--> [Time] Data Loading ($recordCount records): ${(t1_read - t0_read) / 1e9d}%.2f seconds")

    // --- Pipeline Stages Definition ---

    // 2. --- Tokenization ---
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("tokens")

    // 3. --- Stop Words Removal ---
    val stopWordsRemover = new StopWordsRemover()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("filtered_tokens")


    // --- CHUYỂN ĐỔI GIỮA CÁC VECTORIZER ---

    // --- OPTION A: TF-IDF + Normalizer (Được kích hoạt cho Yêu cầu 3 & 4) ---

    // 4. HashingTF
    val hashingTF = new HashingTF()
      .setInputCol(stopWordsRemover.getOutputCol)
      .setOutputCol("raw_features")
      .setNumFeatures(numFeatures)

    // 5. IDF
    val idf = new IDF()
      .setInputCol(hashingTF.getOutputCol)
      .setOutputCol("idf_features") // Đổi tên output để đưa vào Normalizer

    // --- [REQ 3] Vector Normalization ---
    // Chuẩn hóa vector (L2 Norm). Giúp tính Cosine Similarity dễ dàng hơn.
    val normalizer = new Normalizer()
      .setInputCol("idf_features")
      .setOutputCol("features") // Đây là output cuối cùng dùng cho Model
      .setP(2.0)

    /* --- OPTION B: Word2Vec (Bài tập 4) ---
    // Word2Vec học cách biểu diễn từ thành vector sao cho các từ đồng nghĩa nằm gần nhau.
    val word2Vec = new Word2Vec()
      .setInputCol("filtered_tokens")
      .setOutputCol("features")
      .setVectorSize(100)
      .setMinCount(0)
    */

    // 6. Define Logistic Regression Stage
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.01)
      .setFeaturesCol("features") // Input col (đến từ Normalizer hoặc Word2Vec)
      .setLabelCol("label")

    // 7. --- Assemble the Pipeline ---

    // Pipeline MỚI (Dùng TF-IDF + Normalizer + LR)
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, stopWordsRemover, hashingTF, idf, normalizer, lr))

    /* Pipeline dùng Word2Vec
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, stopWordsRemover, word2Vec, lr))
    */

    // --- [REQ 2] Measure Train Time ---
    println("\nFitting the NLP pipeline...")
    val t0_train = System.nanoTime()

    val pipelineModel = pipeline.fit(trainingDF)

    val t1_train = System.nanoTime()
    println(f"--> [Time] Pipeline Training: ${(t1_train - t0_train) / 1e9d}%.2f seconds")

    // --- [REQ 2] Measure Process Time ---
    println("\nTransforming data...")
    val t0_process = System.nanoTime()

    val transformedDF = pipelineModel.transform(trainingDF)
    transformedDF.cache()
    val transformCount = transformedDF.count() // Trigger action

    val t1_process = System.nanoTime()
    println(f"--> [Time] Data Processing: ${(t1_process - t0_process) / 1e9d}%.2f seconds")


    // --- [REQ 4] Find Similar Documents (Cosine Similarity) ---
    println("\n--- Finding Similar Documents ---")

    // a. Lấy tài liệu đầu tiên làm mẫu (Query)
    val firstRecord = transformedDF.select("text", "features").head()
    val queryText = firstRecord.getAs[String]("text")
    val queryVector = firstRecord.getAs[Vector]("features")

    println(s"Query Document: ${queryText.substring(0, Math.min(queryText.length, 100))}...")

    // b. Định nghĩa hàm tính độ tương đồng
    // Vì Vector đã được chuẩn hóa (Normalized) ở bước trên, nên Cosine Similarity chính là Tích vô hướng (Dot Product)

    val calculateSimilarity = udf { (targetVector: Vector) =>
      val v1 = queryVector.toArray
      val v2 = targetVector.toArray
      var dotProduct = 0.0
      // Tính tổng (Ai * Bi)
      for (i <- v1.indices) {
        dotProduct += v1(i) * v2(i)
      }
      dotProduct
    }

    // c. Áp dụng tìm kiếm
    val similarDocsDF = transformedDF
      .withColumn("similarity", calculateSimilarity($"features"))
      .filter($"similarity" < 0.9999) // Loại bỏ chính văn bản gốc (độ giống ~ 1.0)
      .sort(desc("similarity"))       // Sắp xếp giảm dần độ giống
      .limit(5)                       // Lấy top 5
      .select("similarity", "text")

    // In kết quả ra màn hình
    println("\nTop 5 Most Similar Documents:")
    val topDocs = similarDocsDF.collect()
    topDocs.foreach { row =>
      println(f"Score: ${row.getAs[Double]("similarity")}%.4f | Content: ${row.getAs[String]("text").take(80)}...")
    }

    // --- Show and Save Results ---
    val n_results = 20
    val results = transformedDF.select("text", "features", "label", "prediction").take(n_results)

    // 8. --- Write Metrics and Results ---

    // Write metrics to log file (Updated for final requirements)
    val log_path = "D:\\ADMIN\\Documents\\Classwork\\spark_labs\\log/lab17_final_metrics.log"
    new File(log_path).getParentFile.mkdirs()
    val logWriter = new PrintWriter(new File(log_path))
    try {
      logWriter.println("--- Performance Metrics (Final Requirements) ---")
      logWriter.println(f"Document Limit set to: $limitDocuments")
      logWriter.println(f"Data Read Time: ${(t1_read - t0_read) / 1e9d}%.2f seconds")
      logWriter.println(f"Pipeline Train Time: ${(t1_train - t0_train) / 1e9d}%.2f seconds")
      logWriter.println(f"Data Process Time: ${(t1_process - t0_process) / 1e9d}%.2f seconds")
      logWriter.println(s"Pipeline Strategy: Tokenizer -> StopWords -> HashingTF -> IDF -> Normalizer -> LR")
      println(s"\nSuccessfully wrote metrics to $log_path")
    } finally {
      logWriter.close()
    }

    // Write results to output file
    val result_path = "D:\\ADMIN\\Documents\\Classwork\\spark_labs\\results/lab17_final_output.txt"
    new File(result_path).getParentFile.mkdirs()
    val resultWriter = new PrintWriter(new File(result_path))
    try {
      resultWriter.println(s"--- NLP Pipeline Final Output ---")

      // Ghi phần tìm kiếm tương đồng
      resultWriter.println("\n--- SIMILARITY SEARCH RESULTS ---")
      resultWriter.println(s"Query Text: ${queryText.take(200)}...")
      resultWriter.println("-" * 50)
      topDocs.foreach { row =>
        resultWriter.println(f"Score: ${row.getAs[Double]("similarity")}%.4f")
        resultWriter.println(s"Content: ${row.getAs[String]("text")}")
        resultWriter.println("-" * 50)
      }

      // Ghi mẫu dữ liệu
      resultWriter.println(s"\n--- PROCESSED DATA SAMPLE ($n_results records) ---")
      results.foreach { row =>
        val text = row.getAs[String]("text")
        val features = row.getAs[org.apache.spark.ml.linalg.Vector]("features")
        val prediction = row.getAs[Double]("prediction")

        resultWriter.println("="*80)
        resultWriter.println(s"Text: ${text.substring(0, Math.min(text.length, 100))}...")
        // Chỉ in một phần vector vì vector 1000 phần tử rất dài
        resultWriter.println(s"Normalized TF-IDF Vector (First 20): ${features.toArray.take(20).mkString(", ")} ...")
        resultWriter.println(s"Prediction: $prediction")
        resultWriter.println("="*80)
      }
      println(s"Successfully wrote final results to $result_path")
    } finally {
      resultWriter.close()
    }

    spark.stop()
    println("Spark Session stopped.")
  }
}