from urllib.request import urlopen
import json
import pymysql

#医疗卫生机构数统计
class ShenZhen_yiliao_data:
        def intoMysql(self):
            conn = pymysql.connect(host='192.168.1.108', user='root', password='root', db='enterprise', charset="utf8")

            cur = conn.cursor()
            # spjg, jbyfkzzxfyz, wz, fybjysz, mzbzsywscwss, yy, cgxjg, zkjbfzysz, qtlb, wsy, sqwsfwzxz, jjzxz, wsshtt
            sql1 = '''
                            create table shenzhen_yiliao_data(
                            id VARCHAR(255) NOT NULL primary key comment '唯一编码',
                            spjg VARCHAR(255) comment '审批机构',
                            jbyfkzzxfyz VARCHAR(255) comment '疾病预防控制中心（防疫站数量）',
                            wz VARCHAR(255) comment '未知（数量）',
                            fybjysz VARCHAR(255) comment '妇幼保健院（所、站数量）',
                            mzbzsywscwss VARCHAR(255) comment '门诊部、诊所、医务室、村卫生室（数量）',
                            yy VARCHAR(255) comment '医院（数量）',
                            cgxjg VARCHAR(255) comment '采供血机构（数量）',
                            zkjbfzysz VARCHAR(255) comment '专科疾病防治院（所、站数量）',
                            qtlb VARCHAR(255) comment '其他类别（数量）',
                            wsy VARCHAR(255) comment '卫生院（数量）',
                            sqwsfwzxz VARCHAR(255) comment '社区卫生服务中心(站数量)',
                            jjzxz VARCHAR(255) comment '急救中心（站数量）',
                            wsshtt VARCHAR(255) comment '卫生社会团体（数量）'
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
                "https://opendata.sz.gov.cn/api/883367298/1/service.xhtml?page=1&rows=2&appKey=8e1452e400624292b51cc56011551032")
            # 获取json对象
            Hjson = json.loads(Html.read())
            # print(Hjson)
            # 获取总记录数
            TotalNum = Hjson['total']
            print("总共有" + str(TotalNum) + "条数据")
            # 获取页数（以一页1000条数据作为分母）
            Page = TotalNum // 2
            print("需查询至第" + str(Page) + "页")
            for page in range(1, Page+2):
                url="https://opendata.sz.gov.cn/api/883367298/1/service.xhtml?page="+str(page)+"&rows=2&appKey=8e1452e400624292b51cc56011551032"
                html=urlopen(url)
                hjson=json.loads(html.read())
                #获取data数据
                data = hjson['data']
                print(data)
                # 1	WSSHTTSL	卫生社会团体（数量）
                # 2	JJZXZSL	急救中心（站数量）
                # 3	SQWSFWZXZSL	社区卫生服务中心(站数量)
                # 4	WSYSL	卫生院（数量）
                # 5	QTLBSL	其他类别（数量）
                # 6	ZKJBFZYSZSL	专科疾病防治院（所、站数量）
                # 7	CGXJGSL	采供血机构（数量）
                # 8	YYSL	医院（数量）
                # 9	MZBZSYWSCWSSSL	门诊部、诊所、医务室、村卫生室（数量）
                # 10	FYBJYSZSL	妇幼保健院（所、站数量）
                # 11	WZSL	未知（数量）
                # 12	JBYFKZZXFYZSL	疾病预防控制中心（防疫站数量）
                # 13	ID	ID
                # 14	SPJG	审批机构
                for row in range(0,2):
                    id = data[row]['ID']
                    # print(id)
                    spjg=data[row]['SPJG']
                    # print(spjg)
                    jbyfkzzxfyz=data[row]['JBYFKZZXFYZSL']
                    # print(jbyfkzzxfyz)
                    wz = data[row]['WZSL']
                    # print(wz)
                    fybjysz=data[row]['FYBJYSZSL']
                    # print(fybjysz)
                    mzbzsywscwss=data[row]['MZBZSYWSCWSSSL']
                    # print(mzbzsywscwss)
                    yy=data[row]['YYSL']
                    # print(yy)
                    cgxjg=data[row]['CGXJGSL']
                    # print(cgxjg)
                    zkjbfzysz=data[row]['ZKJBFZYSZSL']
                    # print(zkjbfzysz)
                    qtlb=data[row]['QTLBSL']
                    # print(qtlb)
                    wsy=data[row]['WSYSL']
                    # print(wsy)
                    sqwsfwzxz=data[row]['SQWSFWZXZSL']
                    # print(sqwsfwzxz)
                    jjzxz=data[row]['JJZXZSL']
                    # print(jjzxz)
                    wsshtt=data[row]['WSSHTTSL']
                    # print(wsshtt)
                    sql2 = '''
                        insert into shenzhen_yiliao_data(id,spjg,jbyfkzzxfyz,wz,fybjysz,mzbzsywscwss,yy,cgxjg,zkjbfzysz,qtlb,wsy,sqwsfwzxz,jjzxz,wsshtt)
                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                          '''
                    try:
                        B = cur.execute(sql2, (id,spjg,jbyfkzzxfyz,wz,fybjysz,mzbzsywscwss,yy,cgxjg,zkjbfzysz,qtlb,wsy,sqwsfwzxz,jjzxz,wsshtt))
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
    spider = ShenZhen_yiliao_data()
    spider.intoMysql()

