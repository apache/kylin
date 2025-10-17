import requests

SERVER = "http://localhost:8000"
API_KEY = "secretkey123"

def list_tools():
    r = requests.get(f"{SERVER}/tools", params={"api_key": API_KEY})
    print(r.json())

def invoke(tool, params):
    payload = {"tool": tool, "parameters": params, "context": {"user": "alice@example.com"}}
    r = requests.post(f"{SERVER}/invoke", json=payload, params={"api_key": API_KEY})
    print(r.json())

if __name__ == "__main__":
    list_tools()
    invoke("echo", {"message": "hello mcp"})
