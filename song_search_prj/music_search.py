from flask import Flask, request, jsonify
import json
from es_util import ES

app = Flask(__name__)

es = ES()


@app.route("/singer", methods=["post"])
def get_by_singer():
    key = "singer"
    print(request)
    value = request.json["singer"]
    size = 20
    res = es.search_specific(key, value, size)
    return jsonify(res)


@app.route("/lyric", methods=["post"])
def get_by_lyric():
    key = "geci"
    value = request.json["lyric"]
    size = 20
    res = es.search_specific(key, value, size)
    return jsonify(res)


@app.route("/composer", methods=["post"])
def get_by_composer():
    key = "composer"
    value = request.json["composer"]
    size = 20
    res = es.search_specific(key, value, size)
    return jsonify(res)


@app.route("/author", methods=["post"])
def get_by_author():
    key = "author"
    value = request.json["author"]
    size = 20
    res = es.search_specific(key, value, size)
    return jsonify(res)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8808)
