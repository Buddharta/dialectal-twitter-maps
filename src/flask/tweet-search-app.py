from flask import Flask, render_template
from flask import request, Response
from fetch_db import *

app = Flask(__name__,template_folder='templates')

## Index for html app route
@app.route('/')
def index():
    return render_template("index.html")

@app.route("/get-data")
async def async_db_query(concept :str):
    data = await query_db(concept)
    return data 

@app.route('/search', methods=["GET","POST"])
def search():
    term=request.args.get('search_query')
    datalist=async_db_query(term)
    return render_template("search.html",data=datalist,term=term)
## Running app in debug mode
if __name__ == '__main__':
    app.run(host='0.0.0.0',port=9000,debug=True)
