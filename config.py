from airtable import Airtable
#ADD NAME FROM AIRTABLE ACCOUNTS FIRST
Name = 'Bragg'

def get_config():
    airtable = Airtable('Key1', 'Server','key2')
    account = airtable.search('Name', Name)[0]
    if account:
        return {"marketplace_id" : "Markert1",
                "bucket_name" : "Bucket1", 
                "name" : Name, 
                "seller_id" : account['fields']['seller-id'] }

print(get_config())