from dotenv import load_dotenv
import schwabdev
import os

load_dotenv()

client = schwabdev.Client(os.getenv("app_key"), os.getenv("app_secret"))

ticker = "/SR3Z25"
resp = client.quotes(symbols=ticker).json() 
print(resp)