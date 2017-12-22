#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import config
import re

def hashtag_re(cont):
	return re.findall(r'#[^#]+#',cont)

def name_re(cont):
	return re.findall(r'@([^\s:]+)',cont)

def weibo_hashtag_index(weibo_data,hashtag_index):
	weibo = open(weibo_data,'rb')
	hashtag = open(hashtag_index,'wb')
	hashtag_dic = {}
	for line in weibo:
		lines = line.strip().split('\t')
		cont = ''.join(lines[9:])
		hashtag_list = re.findall(r'#[^#]+#',cont)
		for _ in hashtag_list:
			hashtag_dic[_] = hashtag_dic.get(_,0)+1
	hashtag_dic_sorted = sorted(hashtag_dic.iteritems(),key = lambda asd:asd[1],reverse = True)
	print('hashtag number:{}'.format(len(hashtag_dic_sorted)))
	i = 0
	for _ in hashtag_dic_sorted:
		hashtag.write('{},{},{}\n'.format(i,_[1],_[0]))
		i += 1
	hashtag.close()
	weibo.close()

def weibo_hashtag_report(hashtag_index,weibo_data,weibo_hashtag_feature):
	weibo = open(weibo_data,'rb')
	hashtag = open(hashtag_index,'rb')
	weibo_hash_data = open(weibo_hashtag_feature,'wb')
	
	hashtag_dict = {}	
	for line in hashtag:
		lines = line.strip().split(',')
		hashtag_dict[','.join(lines[2:]).decode('utf-8')] = lines[0]
	hashtag.close()

	for line in weibo:
		lines = line.strip().split('\t')
		cont = ''.join(lines[6:])
		uid = lines[0]
		pt = lines[1]
		uid_list = re.findall(r'@([^\s:]+)',cont)
		hashtag_list = re.findall(r'#[^#]+#',cont)
		if len(uid_list) > 0:
			hashtag_list_first = re.findall(r'#[^#]+#',cont.split(uid_list[-1])[-1])
			if len(hashtag_list_first) > 0:
				for _ in hashtag_list_first:
					try:
						weibo_hash_data.write('{},{},{},1,{}\n'.format(uid,pt,_,','.join(uid_list)))
					except Exception,e:
						print('{},{}\n'.format(_,e))
						
		else:
			if len(hashtag_list) > 0:
				for _ in hashtag_list:
					weibo_hash_data.write('{},{},{},0\n'.format(uid,pt,_))
	weibo.close()
	weibo_hash_data.close()	

def weibo_hashtag_feature(weibo_name,weibo_data):
	name_uid = {}
	with open(weibo_name,'rb') as f:
		data = f.readlines()
		for _ in data:
			temp = _.strip().split('\t')
			name_uid[temp[1]] = name_uid[temp[0]]
	
	with open(weibo_data,'rb') as f:
		data = f.readlines()
		for _ in data:
			temp = _.strip().split('\t')
			
def weibo_hive_feature(weibo_data_original,weibo_data_report,weibo_name,weibo_feature):
	weibo_name_uid = {}
	with open(weibo_name,'rb') as f:
		data = f.readlines()
		for _ in data:
			temp = _.strip().split('\t')
			weibo_name_uid[temp[1].decode('utf-8')] = temp[0]
	
	
	#uid,pt,max(nfri),max(nfans),max(nrply),max(nfwd),wb_mid,wb_r_mid,wb_r_uid,wb_msg_type,cont	
	original_mid_hashtag = {}
	original_mid_uid_time = {}
	def data_parse(content):
		temp = content.strip().split('\t')
		cont = '\t'.join(temp[10:])
		hashtag_list = hashtag_re(cont)
		name_list = name_re(cont)
		uid = temp[0]
		pt = temp[1]
		wb_mid = temp[6]
		wb_r_mid = temp[7]
		wb_r_uid = temp[8]
		return uid,pt,wb_mid,wb_r_mid,wb_r_uid,hashtag_list,name_list
		
	with open(weibo_data_original,'rb') as f:
		data = f.readlines()
		for _ in data:
			uid,pt,wb_mid,wb_r_mid,wb_r_uid,hashtag_list,name_list = data_parse(_)
			original_mid_hashtag[wb_mid] = hashtag_list
			original_mid_uid_time[wb_mid] = [uid,pt]
	print('weibo_data_original_end')	
	original_mid_hashtag_report = {}
	with open(weibo_data_report,'rb') as f:
		data = f.readlines()
		for _ in data:
			uid,pt,wb_mid,wb_r_mid,wb_r_uid,hashtag_list,name_list = data_parse(_)
			if wb_r_mid in original_mid_hashtag:
				if wb_r_mid not in original_mid_hashtag_report:
					original_mid_hashtag_report[wb_r_mid] = []
				if len(name_list) == 0:
					original_mid_hashtag_report[wb_r_mid].append([pt,wb_r_uid,uid])
				else:
					report_temp = original_mid_hashtag_report[wb_r_mid]
					uid_report = [pt,wb_r_uid]
					for name in name_list[::-1]:
						
						if name.decode('utf-8') in weibo_name_uid:
							uid_report.append(weibo_name_uid[name.decode('utf-8')])
					uid_report.append(uid)
					report_temp.append(uid_report)
					original_mid_hashtag_report[wb_r_mid] = report_temp
	print('weibo_data_report')
	with open(weibo_feature,'wb') as w:
		for key,value in original_mid_hashtag_report.iteritems():
			message_id = key
			report_list = value
			root_user_id,publish_time = original_mid_uid_time[key]
			retweet_number = len(report_list)
			hashtag_list_temp = original_mid_hashtag[key]
			message_times = len(hashtag_list_temp)
			if(message_times == 0):
				continue
			if(int(config.split) == 1):
				message_times = 1
			for i in range(message_times):
			
				w.write('{}_{}\t{}\t{}\t{}\t{}:0'.format(message_id,hashtag_list_temp[i],root_user_id,publish_time,retweet_number,root_user_id))
				for _ in report_list:
					pt,uid_report = int(_[0]),_[1:]
					if int(pt) < int(publish_time):
						pt += 12 * 3600
					w.write(' {}:{}'.format('/'.join(uid_report), pt - int(publish_time)))
				w.write('\n')

	
if __name__ == '__main__':
	#weibo_hashtag_index(config.weibo_data_hive,config.weibo_hashtag_index)
	#weibo_hashtag_report(config.weibo_hashtag_index,config.weibo_data_hive,config.weibo_hashtag_feature)		
	#hashtag_number(config.weibo_data_hive)	
	#weibo_original(config.weibo_data_hive_path,config.weibo_original,config.weibo_report)	
	weibo_hive_feature(config.weibo_original,config.weibo_report,config.weibo_name,config.weibo_feature)
