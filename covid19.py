from urllib.request import urlopen
import json
import pymysql

#新冠肺炎个体信息
class COVID19:
        def intoMysql(self):
            conn = pymysql.connect(host='192.168.1.108', user='root', password='root', db='enterprise', charset="utf8")

            cur = conn.cursor()
            #fbrq,fbusj,blh,nl,xb,jzd,yqtblgx,zwhsjqj,lssj,fbingsj,rysj,rbyy,bk,bzzzytjd
            sql1 = '''
                            create table covid19(
                            blh VARCHAR(255) NOT NULL primary key comment '病例号',
                            fbrq VARCHAR(255) comment '发布日期',
                            fbusj VARCHAR(255) DEFAULT NULL comment '发布时间',
                            nl VARCHAR(255) comment '年龄',
                            xb VARCHAR(255) comment '性别',
                            jzd VARCHAR(255) comment '居住地',
                            yqtblgx VARCHAR(255) comment '与其他病例关系',
                            zwhsjqj VARCHAR(255) comment '在武汉时间（区间）',
                            lssj VARCHAR(255) comment '来深时间',
                            fbingsj VARCHAR(255) comment '发病时间',
                            rysj VARCHAR(255) comment '入院时间',
                            rbyy VARCHAR(255) comment '染病原因',
                            bk VARCHAR(255) comment '病况',
                            bzzzytjd VARCHAR(255) comment '备注（症状与途径地）'
                            )
                            DEFAULT CHARSET=utf8;
                    '''

            try:
                A = cur.execute(sql1)
                conn.commit()
                print('创建表成功!')
            except:
                print("创建表错误,或表已存在!")


            # 获取网页对象
            Html = urlopen(
                "https://opendata.sz.gov.cn/api/29200_01503668/1/service.xhtml?page=1&rows=200&appKey=d8445b1959af407b9697b67798330930")
            # 获取json对象
            Hjson = json.loads(Html.read())
            # print(Hjson)
            # 获取总记录数
            TotalNum = Hjson['total']
            print("总共有" + str(TotalNum) + "条数据")
            # 获取页数（以一页1000条数据作为分母）
            Page = TotalNum // 200
            print("需查询至第" + str(Page+1) + "页")
            # for page in range(1, Page+2):
            #     url="https://opendata.sz.gov.cn/api/29200_01503668/1/service.xhtml?page="+str(page)+"&rows=200&appKey=d8445b1959af407b9697b67798330930"
            #     html=urlopen(url)
            #     hjson=json.loads(html.read())
            #     #获取data数据
            #     data = hjson['data']
            #     print(data)
            #
            #     for row in range(0,200):
            #         fbrq=data[row]['fbrq']
            #         fbusj=data[row]['fbusj']
            #         blh=data[row]['blh']
            #         nl=data[row]['nl']
            #         xb=data[row]['xb']
            #         jzd=data[row]['jzd']
            #         yqtblgx=data[row]['yqtblgx']
            #         zwhsjqj=data[row]['zwhsjqj']
            #         lssj=data[row]['lssj']
            #         fbingsj=data[row]['fbingsj']
            #         rysj=data[row]['rysj']
            #         rbyy=data[row]['rbyy']
            #         bk=data[row]['bk']
            #         bzzzytjd = data[row]['bzzzytjd']
            #         sql2 = '''
            #             insert into covid19(fbrq,fbusj,blh,nl,xb,jzd,yqtblgx,zwhsjqj,lssj,fbingsj,rysj,rbyy,bk,bzzzytjd)
            #             values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            #               '''
            #         try:
            #             B = cur.execute(sql2, (fbrq,fbusj,blh,nl,xb,jzd,yqtblgx,zwhsjqj,lssj,fbingsj,rysj,rbyy,bk,bzzzytjd))
            #             conn.commit()
            #             print("第"+str(page)+"页第"+str(row)+"条数据插入成功！")
            #         except:
            #             print("第" + str(page) + "页第" + str(row) + "条数据插入失败！")
            #     print("第"+str(page)+"页全部插入成功!")
            # print("结束了!")
            conn.commit()
            cur.close()
            conn.close()

if __name__ == "__main__":
    spider = COVID19()
    spider.intoMysql()

