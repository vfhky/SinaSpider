# encoding=utf-8

# __________________________________________
#   增加了向Mysql数据库中保存pipeline
#   需要有MysqlDB,同时修改Spider文件，增加Item类所有变量的if else的返回值，使得可以标准化存储       
#   Updated by Charles Yan
#   Date:2017.1.4
#   Added Mysql insert method
# ------------------------------------------

#import pymongo
from items import InformationItem, TweetsItem, RelationshipsItem
import MySQLdb

class MysqlDBPipleline(object):
    def __init__(self):
        self.count = 1
        self.conn = MySQLdb.connect(
                host='localhost',
                port=3306,
                user='vfhky',
                #这里填写密码
                passwd='8888',
                db='SinaWeibo',
                charset='utf8',
                )
        self.cur = self.conn.cursor()

    def process_item(self, item, spider):
        """ 判断item的类型，并作相应的处理，再入数据库 """
        if isinstance(item, RelationshipsItem):
            try:
                print("***********at beginning of saving**********")
                print(dict(item))
                sql = ''
                sql+=str('INSERT INTO SinaWeibo.Relationship (`Host1`,`Host2`) ')
                sql+=str(' Values(\'' )
                sql+=str(item['Host1'])
                print(sql)
                sql+=str('\', \'')
                sql+=str(item['Host2'])
                sql+=str('\')')
                print("*********** SQL SYNTAX *********** ")
                print(''.join(sql))
                self.cur.execute(sql)
                self.conn.commit()
                print("saved")
                self.count = self.count +1
                print(self.count)
            except Exception:
                pass
        elif isinstance(item, TweetsItem):
            try:
                print("***********at beginning of saving TweetsItem**********")
                
                sql = ''
                print item
                print("1111111111111111111")
                #print item['Co_oridinates']
                #print("3333333333333333333")
                #sql+=str('INSERT INTO SinaWeibo.Tweets (`weibo_id`,`User_id`,`Content`,`Pubtime`,`Coordinates`,`Tools`,`Likes`,`Comments`,`Transfers`) ')
                sql+=str('INSERT INTO SinaWeibo.Tweets (`weibo_id`,`User_id`,`Content`,`Pubtime`,`Tools`,`Likes`,`Comments`,`Transfers`) ')
                sql+=str(' Values(\'' )
                
                sql+=str(item['_id'])
                sql+=str('\', \'')
                
                sql+=str(item['ID'])
                sql+=str('\', \'')
                
                sql+=item['Content']
                sql+=str('\', \'')
                
                sql+=item['PubTime']
                sql+=str('\', \'')
               
                #sql+=str(item['Co_oridinates'])
                #sql+=str('\', \'')
                
                sql+=item['Tools']
                sql+=str('\', \'')
                
                sql+=str(item['Like'])
                sql+=str('\', \'')
                
                sql+=str(item['Comment'])
                sql+=str('\', \'')
                
                sql+=str(item['Transfer'])
                sql+=str('\')')
                print("*********** SQL SYNTAX *********** ")
                print(''.join(sql))
                self.cur.execute(sql)
                self.conn.commit()
                print("saved")
                self.count = self.count +1
                print(self.count)
            except Exception:
                pass
        elif isinstance(item, InformationItem):
            try:
                print("***********at beginning of saving InformationItem**********")
                #print item
                print str(item['_id'])
                print item['NickName']
                print item['Gender']
                print item['Province']
                print item['City']
                #print item['Signature']
                print item['Birthday']
                print item['Num_Tweets']
                print item['Num_Follows']
                print item['Num_Fans']
                #print item['Sex_Orientation']
                #print item['Marriage']
                #print item['VIPlevel']
                print item['URL']
                
                sql = ''
                sql+=str('INSERT INTO SinaWeibo.Information (`User_id`,`NickName`,`Gender`,`Province`,`City`,`Birthday`,`Num_Tweets`,`Num_Follows`,`Num_Fans`,`URL`) ')
                sql+=str(' Values(\'' )
                
                sql+=str(item['_id'])
                sql+=str('\', \'')
                
                sql+=item['NickName']
                sql+=str('\', \'')
                
                sql+=item['Gender']
                sql+=str('\', \'')
                
                sql+=item['Province']
                sql+=str('\', \'')
                
                sql+=item['City']
                sql+=str('\', \'')
                
                #没有
                #sql+=str(item['Signature'])
                #print("+++++++++eeeeeeeeeeeeeee")
                #sql+=str('\', \'')
                
                sql+=str(item['Birthday'])
                sql+=str('\', \'')
                
                sql+=str(item['Num_Tweets'])
                sql+=str('\', \'')
                
                sql+=str(item['Num_Follows'])
                sql+=str('\', \'')
                
                sql+=str(item['Num_Fans'])
                sql+=str('\', \'')
                
                #没有
                #sql+=str(item['Sex_Orientation'])
                #print("+++++++++jjjjjjjjjjjjjjjjj")
                #sql+=str('\', \'')
                #没有
                #sql+=str(item['Marriage'])
                #print("+++++++++lllllllllllllll")
                
                #sql+=str('\', \'')
                #没有
                #sql+=str(item['VIPlevel'])
                #print("+++++++++nnnnnnnnnnnnnnnn")
                #sql+=str('\', \'')
                
                sql+=str(item['URL'])
                sql+=str('\')')
               
                print("*********** SQL SYNTAX *********** ")
                print(''.join(sql))
                self.cur.execute(sql)
                self.conn.commit()
                print("saved")
                self.count = self.count +1
                print(self.count)
            except Exception:
                pass
            
            ##在Java开发中，Dao连接会对内存溢出，需要定时断开重连，这里不清楚是否需要，先加上了
            if self.count == 1000:
                print("try reconnecting")
                self.count = 0
                self.cur.close()
                self.conn.close()
                self.conn = MySQLdb.connect(
                    host='localhost',
                    port=3306,
                    user='vfhky',
                    passwd='8888',
                    db='SinaWeibo',
                    charset='utf8',
                )
                self.cur = self.conn.cursor()
                print("reconnect")
                
        return item
    


#class MongoDBPipleline(object):
#    def __init__(self):
#        clinet = pymongo.MongoClient("localhost", 27017)
#        db = clinet["Sina"]
#        self.Information = db["Information"]
#        self.Tweets = db["Tweets"]
#        self.Relationships = db["Relationships"]
#
#    def process_item(self, item, spider):
#        """ 判断item的类型，并作相应的处理，再入数据库 """
#        if isinstance(item, RelationshipsItem):
#            try:
#                self.Relationships.insert(dict(item))
#            except Exception:
#                pass
#        elif isinstance(item, TweetsItem):
#            try:
#                self.Tweets.insert(dict(item))
#            except Exception:
#                pass
#        elif isinstance(item, InformationItem):
#            try:
#                self.Information.insert(dict(item))
#            except Exception:
#                pass
#        return item
