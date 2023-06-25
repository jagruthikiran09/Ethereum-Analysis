import pyspark
import time

sc = pyspark.SparkContext()

#part-b top 10 services
top_addresses = ["0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444", 
                    "0xfa52274dd61e1643d2205169732f29114bc240b3",
                    "0x7727e5113d1d161373623e5f49fd568b4f543a9e",
                    "0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef",
                    "0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8",
                    "0xbfc39b6f805a9e40e77291aff27aee3c96915bdd",
                    "0xe94b04a0fed112f3664e45adb2b8915693dd5ff3",
                    "0xbb9bc244d798123fde783fcc1c72d3bb8c189413",
                    "0xabbb6bebfa05aa13e908eaa492bd7a8343760477",
                    "0x341e790174e3a4d35b65fdc067b6b5634a61caea"]


def clean(line):
    try:
        fields = line.split(',')
        if len(fields) == 7:  #transactions dataset
            str(fields[2])  #address field in transactions dataset
            if int(fields[3]) == 0:
                return False
        elif len(fields) == 5:  
            str(fields[0])  
        else:
            return False
        return True
    except:
        return False

def mapper(line):
    try:
        fields = line.split(',')
        timestamp1 = int(fields[6])
        year = time.strftime('%Y', time.gmtime(timestamp1))
        key = (fields[2], year)

        gas_supply = int(fields[4])
        if key[0] in top_addresses:
            return (key, (gas_supply, 1))
        else:
            return ("temp", (1, 1))
    except:
        pass

cleaned_transactions = sc.textFile('/data/ethereum/transactions').filter(clean)

mapped_transactions = cleaned_transactions.map(mapper)
keyed_transactions = mapped_transactions.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))
print("Address", "Year", "Total Gas", "Total number")
for row in keyed_transactions.collect():
    print('{0},{1},{2},{3}'.format(row[0][0], row[0][1], row[1][0], row[1][1]))  
