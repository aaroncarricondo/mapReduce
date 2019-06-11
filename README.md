## Simple MapReduce with IBM Cloud

This is a map and reduce program that executes two types of counting words:
- WordCount: Count the total number of words in the text file.
- CountWords: Count the number of each word that appear in the text file.

### Example diagram:

![alt text](http://errequeerre.es/wp-content/uploads/2017/08/MapReduceWordCountOverview1.png)

In this case, the shuffle step is not done.

### Execution:

  1.  First you need to have your configuration file with all the URLs in the same folder as one of the 3 execution files:
      - orchestrator.py: Executes WordCount or CountWords based on your input.
      - word_count.py: Only executes WordCount.
      - count_word.py: Only executes CountWords.
	  
```
It's very important to have an entry for RabbitMQ, Cloud Object Storage (COS) and Cloud Functions
in this file in order to execute the application. 
```
  2.  Upload the text file you want to parse to IBM Cloud Object Storage.
  
  3.  Execute one of the three files mentioned above:
  
      ```
      python3 orchestrator.py (number_of_chunks) (file_name)
      
      or
      
      python3 word_count.py (number_of_chunks) (file_name)
      
      or
      
      python3 count_word.py (number_of_chunks) (file_name)
      ```

