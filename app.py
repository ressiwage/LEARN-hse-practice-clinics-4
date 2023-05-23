from flask import Flask, url_for, render_template, request, jsonify, redirect
from markupsafe import Markup
import pandas as pd
import os, utils, json, random
from waitress import serve
import configparser
import mysql.connector as connector
import socket
socket.setdefaulttimeout(600) # seconds
dir_path = os.path.dirname(os.path.realpath(__file__))
config = {'host':'45.95.202.187', 'port':8080, 'user':'root', 'password':'1048576power'}
cnx = connector.connect(**config)

config = configparser.ConfigParser()
config.read('config.ini')

if config['WEB_APP'].get('template_folder') != None:
    app = Flask(__name__, instance_relative_config=True,  template_folder=config['WEB_APP']['template_folder'])
else:
    app = Flask(__name__)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
app.config["CACHE_TYPE"] = "null"

@app.after_request
def add_header(r):
    """
    Add headers to both force latest IE rendering engine or Chrome Frame,
    and also to cache the rendered page for 10 minutes.
    """
    r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    r.headers['Cache-Control'] = 'public, max-age=0'
    return r


tags = {'otolaryngology':'отоларинголог', 
        'dentist':'стоматолог', 
        'osteopathy':'остеопат', 
        'ophthalmology':'офтальмолог', 
        'endocrinology':'эндокринолог', 
        'surgery':'хирург', 
        'neurology':'невролог', 
        'vaccination':'вакцинация', 
        'paediatrics':'педиатр',  
        'psychiatry':'психиатр',  
        'cardiology':'кардиолог', 
        'gastroenterology':'гастроэнтеролог', 
        'gynaecology':'гинеколог', 
        'oncology':'онколог', 
        'stomatology':'стоматология', 
        'trauma':'травматолог', 
        'physiotherapy':'физиотерапевт', 
        'urology':'уролог', 
        'orthopaedics':'ортопед', 
        'dermatology':'дерматолог'}



@app.route("/post", methods=['POST'])
def post():
    utils.edit_sql(cnx, dict(request.form)['form-osmid'].replace('osmid: ',''), dict(request.form)['form-specialities'], dict(request.form)['form-name'])
    return redirect('/')

@app.route("/create", methods=['POST'])
def create():
    utils.create_sql(cnx, random.randint(100000000000,1000000000000) , dict(request.form))
    
    return redirect('/')

@app.route("/mark", methods=['POST'])
def mark():
    osmid = json.loads(request.data)['osmid']
    utils.mark_sql(cnx, osmid)
    
    return redirect('/')

def filter_(x, specs):
    if x['healthcare:speciality']=='nan' or x['healthcare:speciality']==None:
        return False
    
    for i in specs:
        key_ = [ind for ind in tags if tags[ind]==i][0]
        if key_ not in x['healthcare:speciality'].split(';'):
            print(key_, x['healthcare:speciality'].split(';'))
            return False
    return True

@app.route("/", methods=['POST', 'GET'])
def main():
    content = utils.sql_to_gpd(cnx, 'clinics')
    content2 = utils.sql_to_gpd(cnx, 'markers')
    

    
    if request.method == 'POST':
        
        specs=request.form.getlist('specialities')
        if specs != []:
            content = content[content.apply(lambda x: filter_(x, specs), axis=1)]
            content2 = content2[content2.apply(lambda x: filter_(x, specs), axis=1)]
    json_ = utils.mini_df(content).to_json()  
    json2 = utils.mini_df(content2).to_json()  
    return render_template('test.html', checkboxes=sorted(set(tags.values())), data=Markup(json_), mark_data=Markup(json2))

if __name__ == "__main__":
    # app.run(debug=True)
    serve(app, host='0.0.0.0', port=config['SERVER']['port'])