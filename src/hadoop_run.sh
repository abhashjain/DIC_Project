javac TFIDF.java -cp $(hadoop classpath)
jar cf TFIDF.jar TFIDF*.class
hadoop jar TFIDF.jar TFIDF input
