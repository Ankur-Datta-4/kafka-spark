from kafka import KafkaConsumer
import json
import sys

 
def main():
    
    consumer = KafkaConsumer(
    sys.argv[1],
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    price_map={} # state:(min,max,count)
    # loop through each line of stdin
    for message in consumer:
        data=message.value
        if(data.get('message')):
            break
        else:
            try:
                if(price_map.get(data["state"])):
                    price_map[data["state"]]=[price_map[data["state"]][0]+float(data["min"]),price_map[data["state"]][1]+float(data["max"]),price_map[data["state"]][2]+1]
                else:
                    price_map[data["state"]]=[float(data["min"]),float(data["max"]),1]
            except Exception as e:
                sys.stderr.write("Error occured: %s" % e)
                sys.stderr.write(f"Data: {message}")
                continue
    ## after reading all messages
    ## print out {state:{min,max}}
    ## rounded to two places
    output={}
    for state in sorted(list(price_map.keys())):
        # print out avg rounded off
        count=price_map[state][2]
        output[state]={"Min":round(price_map[state][0]/count,2),"Max":round(price_map[state][1]/count,2)}

    print(json.dumps(output))
 
if __name__ == "__main__":
        main()

