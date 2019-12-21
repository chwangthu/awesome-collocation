from flask import Flask, render_template, request, redirect, url_for
from flask_paginate import Pagination, get_page_args
from gensim.models import word2vec
from scipy.spatial.distance import cosine
from eswraper import ESWraper

POS_dict = {
    '名词':'n', '人名':'np', '地名':'ns', '机构名':'nl', '其它专名':'nz', '数词':'m', '量词':'q', '数量词':'mq', '时间词':'t', '方位词':'f', '处所词':'s', '动词':'v', '形容词':'a', '副词':'d', '前接成分':'h', '后接成分':'k', '习语':'i', '简称':'j', '代词':'r', '连词':'c', '介词':'p', '助词':'u', '语气助词':'y', '叹词':'e', '拟声词':'o', '语素':'g', '标点':'w', '其它':'x', '能愿动词':'vm', '趋向动词':'vd'
}

app = Flask(__name__)

es = ESWraper()

model = word2vec.Word2Vec.load("../model/word2vec.model")
print("Model loaded")

@app.route('/')
def index_page():
    return render_template("index.html")

result = list()
@app.route('/result/')
def show_result():
    page, per_page, offset = get_page_args(page_parameter='page',
                                           per_page_parameter='per_page')
    pagination_docs = get_docs(result, offset=offset, per_page=per_page)
    pagination = Pagination(page=page, per_page=per_page, total=len(result),
                            css_framework='bootstrap4')
    return render_template('result.html',
                           num_docs=len(result),
                           docs=pagination_docs,
                           page=page,
                           per_page=per_page,
                           pagination=pagination,
                           )

@app.route("/query", methods=["POST", "GET"])
def handle_query():
    form_data = request.form
    keywords = form_data.get('keywords').split(" ")
    win_size = int(form_data.get('window_size'))

    POS = [POS_dict[x] for x in form_data.getlist('POS')]
    print(POS)

    global result
    result = list()
    result = retrieve_docs(es, keywords, win_size, POS)
    result = [x[0] for x in result]
    return redirect(url_for('show_result', result=result))
    
    

def get_docs(docs, offset=0, per_page=10):
    return docs[offset: offset+per_page]

def get_window(win_data):
    return "".join(map(lambda x: x[:x.index('/')], win_data))

def retrieve_docs(es, keywords, win_size, POS=None):
    result = set()
    for keyword in keywords:
        res = retrieve_single(es, [keyword], win_size, POS)
        result = res | result
    result = list(result)
    result = sorted(result, key=lambda x: x[1])
    return list(result)

def retrieve_single(es, keywords, win_size, POS=None):
    raw_docs = es.search_docs(keywords, POS)['hits']['hits']
    docs = [r['_source']['content'] for r in raw_docs]
    result = set()
    print(len(docs))
    for doc in docs:
        word_list = doc.split(" ")
        for idx, word in enumerate(word_list):
            w = word[:word.index('/')]
            if w in keywords:
                win_data = word_list[max(0, idx-win_size): idx+1]
                if(len(win_data) > 0):
                    window = get_window(win_data)
                    if(window != w):
                        result.add( (window, relation(keywords[0], word_list[max(0, idx-win_size)])) )
                win_data = word_list[idx: min(idx+win_size, len(word))]
                if(len(win_data) > 0):
                    window = get_window(win_data)
                    if(window != w):
                        result.add( (window, relation(keywords[0], word_list[min(idx+win_size, len(word))])) )
                # result.append( ' '.join(word_list[max(0, idx-win_size): idx+1]) )
    # print(result)
    return result

def relation(word1, word2):
    word2 = word2[:word2.index('/')]
    if (word1 not in model.wv.vocab) or (word2 not in model.wv.vocab):
        print('not in wv', word1, word2)
        return 2.0
    return cosine(model.wv[word1], model.wv[word2])
        


if __name__ == "__main__":
    app.run(debug=True)
    # retrieve_docs(es, ["喜欢"], 1)