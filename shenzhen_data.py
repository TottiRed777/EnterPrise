from urllib.request import urlopen
import json
import pymysql

#商事主体信息
class ShenZhen_data:
        def intoMysql(self):
            conn = pymysql.connect(host='192.168.1.108', user='root', password='root', db='enterprise', charset="utf8")

            cur = conn.cursor()

            sql1 = '''
                            create table shenzhen_data(
                            id VARCHAR(255) NOT NULL primary key comment '唯一编码',
                            entity_name VARCHAR(255) comment '企业名称',
                            entity_phone VARCHAR(255) DEFAULT NULL comment '联系电话',
                            entity_no VARCHAR(255) comment '注册号',
                            entity_code VARCHAR(255) comment '组织机构代码',
                            jycs VARCHAR(255) comment '经营场所',
                            jyfw VARCHAR(255) comment '经营范围',
                            zczb VARCHAR(255) comment '注册资本',
                            clrq VARCHAR(255) comment '成立日期',
                            djjgdh VARCHAR(255) comment '登记机构代号',
                            hzrq VARCHAR(255) comment '核准/备案日期',
                            yyzt VARCHAR(255) comment '企业状态',
                            bz VARCHAR(255) comment '备注',
                            bizhong VARCHAR(255) comment '币种',
                            year VARCHAR(255) DEFAULT NULL comment '企业年报时间'
                            )
                            DEFAULT CHARSET=utf8;
                    '''

            try:
                A = cur.execute(sql1)
                conn.commit()
                print('创建表成功!')
            except:
                print("创建表错误,表或已存在!")


            # 获取网页对象
            Html = urlopen(
                "https://opendata.sz.gov.cn/api/1564501785/1/service.xhtml?page=1&rows=1000&appKey=d441d1168cfd447ebbbe4cb0b966e3d7")
            # 获取json对象
            Hjson = json.loads(Html.read())
            # print(Hjson)
            # 获取总记录数
            TotalNum = Hjson['total']
            print("总共有" + str(TotalNum) + "条数据")
            # 获取页数（以一页1000条数据作为分母）
            Page = TotalNum // 1000
            print("需查询至第" + str(Page) + "页")
            for page in range(5206, Page+2):
                url="https://opendata.sz.gov.cn/api/1564501785/1/service.xhtml?page="+str(page)+"&rows=1000&appKey=d441d1168cfd447ebbbe4cb0b966e3d7"
                html=urlopen(url)
                hjson=json.loads(html.read())
                #获取data数据
                data = hjson['data']
                print(data)
                # RECORDID	String	唯一编码
                # ZCH		String	注册号
                # ZZJGDM	String	组织机构代码
                # QYMC		String	企业名称
                # JYCS		String	经营场所
                # JYFW		String	经营范围
                # ZCZB		String	注册资本
                # CLRQ		String	成立日期
                # DJJGDH	String	登记机构代号
                # HZRQ		String	核准/备案日期
                # YYZT		String	企业状态
                # BZ		String	备注
                # BIZHONG	String	币种
                for row in range(0,1000):
                    recordid=data[row]['RECORDID']
                    zch=data[row]['ZCH']
                    zzjgdm=data[row]['ZZJGDM']
                    qymc=data[row]['QYMC']
                    jycs=data[row]['JYCS']
                    jyfw=data[row]['JYFW']
                    zczb=data[row]['ZCZB']
                    clrq=data[row]['CLRQ']
                    djjgdh=data[row]['DJJGDH']
                    hzrq=data[row]['HZRQ']
                    yyzt=data[row]['YYZT']
                    bz=data[row]['BZ']
                    bizhong=data[row]['BIZHONG']
                    sql2 = '''
                        insert into shenzhen_data(id,entity_name,entity_no,entity_code,jycs,jyfw,zczb,clrq,djjgdh,hzrq,yyzt,bz,bizhong)
                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                          '''
                    try:
                        B = cur.execute(sql2, (recordid,qymc,zch,zzjgdm,jycs,jyfw,zczb,clrq,djjgdh,hzrq,yyzt,bz,bizhong))
                        conn.commit()
                        print("第"+str(page)+"页第"+str(row)+"条数据插入成功！")
                    except:
                        print("第" + str(page) + "页第" + str(row) + "条数据插入失败！")
                print("第"+str(page)+"页全部插入成功!")
            print("结束了!")
            conn.commit()
            cur.close()
            conn.close()

if __name__ == "__main__":
    spider = ShenZhen_data()
    spider.intoMysql()

