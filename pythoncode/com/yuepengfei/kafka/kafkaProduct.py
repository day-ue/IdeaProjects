from kafka import KafkaProducer
import time
import com.yuepengfei.spider.parseHttp as ph

class KafkaProduct:

    producer = KafkaProducer(bootstrap_servers='192.168.240.131:9092', value_serializer=str.encode)
    set1 = set("http://spark.apache.org/")
    url = "http://spark.apache.org/"
    dep = 0

    def html2Kafka(url):
        KafkaProduct.dep += 1
        html = ph.download(url, num_retries=2, user_agent='wswp')
        result = ph.bs_scriper(html)
        future = KafkaProduct.producer.send('test', result[1])
        future.get(timeout=10)
        for link in result[0]:
            if KafkaProduct.dep > 2:
                break
            if link not in KafkaProduct.set1:
                print(link)
                KafkaProduct.set1.add(link)
                KafkaProduct.html2Kafka(link)
        KafkaProduct.dep -= 1


if __name__ == "__main__":
    KafkaProduct.html2Kafka(KafkaProduct.url)