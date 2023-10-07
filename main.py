"""
ETL-Query script
"""
import fire
from dynamodb import create_table,list_tables,put_item,get_item,update_item,query_items,scan_items,delete_item,delete_table
import boto3


class QueryCLI:
    def create_table(self):
        """Extract data from source"""
        create_table()
    
    def list_tables(self):
        """Extract data from source"""
        table = list_tables()
        print(table)

    def put_item(self):
        """load data into destination"""
        put_item()
    
    def get_item(self):
        """get data"""
        response = get_item()
        print(response["Item"])
        

    def update_item(self):
        """update data into destination"""
        update_item()

    def query_items(self):
        """query data"""
        response = query_items()
        print(response["Items"])

    def scan_items(self):
        """scan data"""
        scan_items()
    
    def delete_item(self):
        """delete data"""
        delete_item()

    def delete_table(self):
        """delete table"""
        delete_table()


if __name__ == "__main__":
    fire.Fire(QueryCLI)
