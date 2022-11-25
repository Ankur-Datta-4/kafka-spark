#!/usr/bin/env python3
from json import dumps
from kafka import KafkaProducer
import sys
 
def main():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
    count=0
    # loop through each line of stdin
    for line in sys.stdin:
        try:
            count+=1
            items=line.split(",")
            if(line=="EOF\n"):
                producer.send(sys.argv[1],value={'message':'EOF'})
                print('EOF reached')
           
            elif(len(items)>1):
                minPrice=items[-3]
                maxPrice=items[-2]
                state=items[0]
                data={'state':state,'min':minPrice,'max':maxPrice}
                producer.send(sys.argv[1],value=data)
        except Exception as e:
            sys.stderr.write("unable to read log: %s" % e)
            continue
    print("exiting kafka producer")       
    print(f"Read {count} lines")
if __name__ == "__main__":
        main()
