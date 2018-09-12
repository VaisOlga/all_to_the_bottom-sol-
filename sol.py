import requests
import json
import psycopg2
from datetime import timedelta, datetime

nameDB = 'shop'
userDB = 'postgres'
passwordDB = '***'

def inquairy(text, flag):
	conn = psycopg2.connect(dbname=nameDB, user=userDB, password=passwordDB)
	cursor = conn.cursor()
	cursor.execute(text)
	if flag:
		row = cursor.fetchone()
	conn.commit()
	cursor.close()
	conn.close()
	if flag:
		return row

def inquairy_fetchall(text):
	conn = psycopg2.connect(dbname=nameDB, user=userDB, password=passwordDB)
	cursor = conn.cursor()
	cursor.execute(text)
	row = cursor.fetchall()
	conn.commit()
	cursor.close
	return row

def clear_database():
	inquairy('DELETE FROM "carts";', False)
	inquairy('DELETE FROM "pay";', False)
	inquairy('DELETE FROM "success_pay";', False)
	inquairy('DELETE FROM "operations";', False)

def fill_goods():
	inquairy('DELETE FROM "goods";', False)
	inquairy('DELETE FROM "categories";', False)
	with open('logs.txt', 'r') as inp:
		d = {}
		goods = {}
		categories = {}
		for line in inp:
			marr = line.split()
			mip = marr[6]
			murl = marr[7]
			marr_3 = murl.split('/')[3]
			if(marr_3.find('?') != -1):
				oper = marr_3.split('?')[0]
				params = marr_3.split('?')[1].split('&')
				if(oper == 'cart'):
					goods_id = params[0].split('=')[1]
					prev_url = d[mip]
					goods_name = prev_url.split('/')[4]
					category_name = prev_url.split('/')[3]
					categories[category_name] = 0
					goods[goods_name] = [goods_id, category_name]
			d[mip] = murl
	for g in goods:
		text = 'INSERT INTO "goods" ("goods_id", "goods_name", "category_name") VALUES({}, {}, {});'.format(goods[g][0], "'"+g+"'", "'"+goods[g][1]+"'")
		inquairy(text, False)
	for c in categories:
		text = 'INSERT INTO "categories" ("category_name") VALUES({})'.format("'"+c+"'")
		inquairy(text, False)

def fill_database():
	clear_database()
	with open('logs.txt', 'r') as inp:
		for line in inp:
			marr = line.split()
			mdate = marr[2]
			mtime = marr[3]
			mip = marr[6]
			murl = marr[7]
			marr_3 = murl.split('/')[3]
			if(marr_3.find('?') != -1):
				oper = marr_3.split('?')[0]
				params = marr_3.split('?')[1].split('&')
				if(oper == 'cart'):
					goods_id = params[0].split('=')[1]
					amount = params[1].split('=')[1]
					cart_id = params[2].split('=')[1]
					text = 'INSERT INTO "carts" ("cart_id", "amount", "goods_id") VALUES({}, {}, {});'.format(cart_id, amount, goods_id)
					inquairy(text, False)
					text = 'INSERT INTO "operations" ("date_time", "ip", "oper", "param") VALUES({}, {}, {}, {})'.format("'"+mdate+' '+mtime+"'", "'"+mip+"'", "'cart'", cart_id)
					inquairy(text, False)
				elif(oper == 'pay'):
					user_id = params[0].split('=')[1]
					cart_id = params[1].split('=')[1]
					text = 'INSERT INTO "pay" ("cart_id", "user_id") VALUES({}, {});'.format(cart_id, user_id)
					inquairy(text, False)
					text = 'INSERT INTO "operations" ("date_time", "ip", "oper", "param") VALUES({}, {}, {}, {})'.format("'"+mdate+' '+mtime+"'", "'"+mip+"'", "'pay'", cart_id)
					inquairy(text, False)
			else:
				ma = marr_3.split('_')
				if(ma[0] == 'success' and ma[1] == 'pay'):
					cart_id = ma[2]
					text = 'INSERT INTO "success_pay" ("cart_id") VALUES({});'.format(cart_id)
					inquairy(text, False)
					text = 'INSERT INTO "operations" ("date_time", "ip", "oper", "param") VALUES({}, {}, {}, {})'.format("'"+mdate+' '+mtime+"'", "'"+mip+"'", "'success_pay'", cart_id)
					inquairy(text, False)
				else:
					col = '"date_time", "ip", "oper"'
					val = "'{}', '{}', '{}'".format(mdate + ' ' + mtime, mip, marr_3)
					if(marr_3 == ''):
						text = 'INSERT INTO "operations" ("date_time", "ip", "oper") VALUES({}, {}, {})'.format("'"+mdate+' '+mtime+"'", "'"+mip+"'", "'main_page'")
						inquairy(text, False)
					else:
						text = 'SELECT "category_id" FROM "categories" WHERE "category_name" = {}'.format("'"+marr_3+"'")
						category_id = inquairy(text, True)[0]
						text = 'INSERT INTO "operations" ("date_time", "ip", "oper", "param") VALUES({}, {}, {}, {})'.format("'"+mdate+' '+mtime+"'", "'"+mip+"'", "'category'", category_id)
						inquairy(text, False)

def task7():
	text = 'SELECT "user_id" FROM "pay" WHERE EXISTS(SELECT "cart_id" FROM "success_pay" WHERE pay.cart_id = success_pay.cart_id);'
	users_id = inquairy_fetchall(text)
	res = {i: users_id.count(i) for i in users_id}
	cnt = 0
	for i in res:
		if(res[i] > 1):
			cnt += 1
	return cnt

def task6():
	text = 'SELECT count(DISTINCT "cart_id") FROM carts WHERE NOT EXISTS(SELECT "cart_id" FROM "success_pay" WHERE carts.cart_id = success_pay.cart_id);'
	cnt = inquairy(text, True)[0]
	return cnt

def task5():
	pass

def task4():
	dt1 = datetime.strptime('2018-08-01', '%Y-%m-%d')
	dt2 = dt1 + timedelta(minutes = 59, seconds = 59)
	dtc = datetime.strptime('2018-08-15', '%Y-%m-%d')
	ans = 0
	while dt1 < dtc:
		text = 'SELECT count("date_time") FROM "operations" WHERE {} <= "date_time" AND "date_time" <= {}'.format("'"+str(dt1)+"'", "'"+str(dt2)+"'")
		res = inquairy(text, True)[0]
		ans = max(ans, res)
		dt1 += timedelta(hours = 1)
		dt2 += timedelta(hours = 1)
	return ans

def task3():
	dt1 = datetime.strptime('2018-08-01', '%Y-%m-%d')
	dtc = datetime.strptime('2018-08-15', '%Y-%m-%d')
	dt = { 0: 0, 1: 0, 2: 0, 3: 0}
	while dt1 < dtc:
		dt2 = dt1 + timedelta(hours = 6)
		text = 'SELECT count("date_time") FROM "operations", "categories" WHERE operations.param = categories.category_id AND categories.category_name = {} AND {} <= "date_time" AND "date_time" <= {}'.format("'frozen_fish'", "'"+str(dt1)+"'", "'"+str(dt2)+"'")
		dt[0] += inquairy(text, True)[0]
		dt3 = dt1 + timedelta(hours = 12)
		text = 'SELECT count("date_time") FROM "operations", "categories" WHERE operations.param = categories.category_id AND categories.category_name = {} AND {} <= "date_time" AND "date_time" <= {}'.format("'frozen_fish'", "'"+str(dt2)+"'", "'"+str(dt3)+"'")
		dt[1] += inquairy(text, True)[0]
		dt4 = dt1 + timedelta(hours = 18)
		text = 'SELECT count("date_time") FROM "operations", "categories" WHERE operations.param = categories.category_id AND categories.category_name = {} AND {} <= "date_time" AND "date_time" <= {}'.format("'frozen_fish'", "'"+str(dt3)+"'", "'"+str(dt4)+"'")
		dt[2] += inquairy(text, True)[0]
		dt5 = dt1 + timedelta(hours = 23, minutes = 59, seconds = 59)
		text = 'SELECT count("date_time") FROM "operations", "categories" WHERE operations.param = categories.category_id AND categories.category_name = {} AND {} <= "date_time" AND "date_time" <= {}'.format("'frozen_fish'", "'"+str(dt4)+"'", "'"+str(dt5)+"'")
		dt[3] += inquairy(text, True)[0]
		dt1 += timedelta(days = 1)
	ind = 0
	for i in dt:
		if(dt[i] > dt[ind]):
			ind = i
	return ind

def task2():
	text = 'SELECT operations.ip, count(operations.date_time) FROM "operations", "categories" WHERE operations.param = categories.category_id AND categories.category_name = {} GROUP BY operations.ip;'.format("'fresh_fish'")
	res = inquairy_fetchall(text)
	d = {}
	for i in res:
		try:
			country = requests.get('https://ipapi.co/{}/json/'.format(str(i[0]))).json()['country']
			if(country in d.keys()):
				d[country] += i[1]
			else:
				d[country] = i[1]
		except Exception:
			prost = False
	dd = { 'US': d['US'], 'CN': d['CN'], 'DE': d['DE'], 'GB': d['GB'], 'JP': d['JP']}
	return dd

def task1():
	text = 'SELECT operations.ip, count(operations.date_time) FROM "operations" GROUP BY operations.ip;'.format("'fresh_fish'")
	res = inquairy_fetchall(text)
	d = {}
	for i in res:
		try:
			country = requests.get('https://ipapi.co/{}/json/'.format(str(i[0]))).json()['country']
			if(country in d.keys()):
				d[country] += i[1]
			else:
				d[country] = i[1]
		except Exception:
			prost = False
	dd = { 'US': d['US'], 'CN': d['CN'], 'DE': d['DE'], 'GB': d['GB'], 'JP': d['JP']}
	return dd

def test_time():
	dt1 = datetime.strptime('2018-08-01', '%Y-%m-%d')
	dt2 = dt1 + timedelta(minutes = 59, seconds = 59)
	dtc = datetime.strptime('2018-08-15', '%Y-%m-%d')
	while dt1 < dtc:
		print(dt1, dt2)
		dt1 += timedelta(hours = 1)
		dt2 += timedelta(hours = 1)

def main():
	# fill_goods()
	# fill_database()
	# 
	# print('task1')
	# print(task1())
	# print('task2')
	# print(task2())
	# print('task 3: ' + str(task3()))
	# print('task 4: ' + str(task4()))
	# print('task 6: ' + str(task6()))
	# print('task 7: ' + str(task7()))
	return

if __name__ == '__main__':
	main()
