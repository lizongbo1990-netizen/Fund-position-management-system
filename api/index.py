# api/index.py
from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/', methods=['GET'])
def home():
    return jsonify({"status": "ok", "message": "Hello from Vercel!"})

# 这是关键：Vercel 需要直接导入 app 对象
# 不要写 if __name__ == '__main__': app.run()
