from flask import Flask

app = Flask(__name__)

@app.route("/identity")
def identity_handler():
	pass