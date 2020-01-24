#!/usr/bin/python36

import time
import os
import subprocess as sp

print("\n\n\t\tWelcome to Hadoop Cluster Setup")
print("\t\t-------------------------------------\n\n")
print("Where do you want to setup hadoop cluster(Local or Remote): ",end='')
loc=input()

if loc == "local" or loc == "Local" or loc == "l" or loc == "L":
	i=1
	while int(i)==1:
		print("""\nWhat do you want to setup? 
	1)HADOOP ENVIROMENT
	2)HDFS CLUSTER
	3)MAPRED CLUSTER
	4)CLIENT
	5)EXIT""")
		print("--------------------------------------------------------------------------------")
		print("Your Choice: ", end='')
		cl=input()
		if int(cl) == 1 or cl == "hadoop" or cl == "Hadoop" or cl == "HADOOP" :
			a=sp.getstatusoutput("rpm -q hadoop || rpm -q jdk")		
			if int(a[0])== 0 :			
				print("Hadoop is already installed")
			else:
				sp.getstatusoutput("rpm -ivh /root/jdk-7u79-linux-x64.rpm")
				b=sp.getstatusoutput("rpm -ivh /root/hadoop-1.2.1-1.x86_64.rpm --force")
				if int(b[0]) == 0:
					print("Hadoop is Installed\nNow Setting Up Path")
				else:
					print("Hadoop not Installed")
				sp.getoutput("scp /copyrc /root/.bashrc")
				sp.getoutput("chmod +x /root/.bashrc")
				print("Hadoop has been Installed and Path is Set")
		elif cl == "HDFS" or cl == "Hdfs" or cl == "hdfs" or cl == "h" or int(cl)== 2:
			print("Enter Master's IP: ",end='')
			m_id=input('192.168.43.')
			j=1			
			while int(j) == 1:			
				print("""1)Setup NameNode
2)Setup DataNode""")
				print("Your Choice: ", end='')
				func=input()
				if int(func) == 1:
					sp.getoutput("rm -rf /master")
					sp.getoutput("scp /masterhdfs.xml /etc/hadoop/hdfs-core.xml")
					z=open("/core.xml",'w+')
					z.write("""<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>

<property>
<name>fs.default.name</name>
<value>hdfs://192.168.43.{}:9001</value>
</property>

</configuration>""".format(m_id))
					z.close()
					sp.getoutput("scp /core.xml /etc/hadoop/core-site.xml")
					sp.getoutput("echo -Y | hadoop namenode -format")
					sp.getoutput("hadoop-daemon.sh start namenode")
					time.sleep(4)
					x=sp.getstatusoutput("jps | grep NameNode")
					if int(x[0])== 0:
						print("Namenode Started successfully")
					else:
						print("Failed to start NameNode") 
				if int(func)==2:
					sp.getoutput("rm -rf /data")
					z=open("/core.xml",'w+')
					z.write("""<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>

<property>
<name>fs.default.name</name>
<value>hdfs://192.168.43.{}:9001</value>
</property>

</configuration>""".format(m_id))
					z.close()
					sp.getoutput("scp /slavehdfs.xml /etc/hadoop/hdfs-site.xml")
					sp.getoutput("scp /core.xml /etc/hadoop/core-site.xml")
					sp.getoutput("hadoop-daemon.sh start datanode")
					y=sp.getstatusoutput("jps| grep DataNode")
					if int(y[0])== 0:
						print("DataNode Started")
					else:
						print("Failed to start DataNode")
				print("Do You want to Continue HDFS Cluster Setup(y/n)? ",end='')
				choice=input()
				if choice == "YES" or choice == "Yes" or choice == "yes" or choice == "y":
					os.system("clear")
				elif choice == "NO" or choice == "No" or choice == "no" or choice == "n":
					j=0
					print("Now exiting")
		elif cl == "MAPRED" or cl == "Mapred" or cl == "mapred" or cl == "m" or int(cl)== 3:
			k=1
			while int(k) == 1:
				print("""\n______________________________________________________________________________
\t-----------------WARNING-------------
_________________________________________________________________________________
Make Sure Your /etc/hosts file is configured in the following manner:- \n\n
127.0.0.1   localhost localhost.localdomain localhost4 localhost4.localdomain4
::1         localhost localhost.localdomain localhost6 localhost6.localdomain6

NameNode's IP		nn.lw.com
DataNode1's IP		dn1.lw.com
DataNode2's IP		dn2.lw.com
DataNode3's IP		dn3.lw.com
JobTracker's IP		jt.lw.com
TaskTracker2's IP	tt1.lw.com
TaskTracker1's IP	tt2.lw.com
Client's IP		client.lw.com\n\n""")
				print("""1)Setup Hosts File
2)Setup JobTracker
3)Setup TaskTracker""")
				print("Your Choice: ",end='')
				func = input()
				if int(func) ==	1:
					nn=input('NameNode IP=')
					dn1=input('Datanode1 IP=')
					dn2=input('Datanode2 IP=')
					dn3=input('Datanode3 IP=')
					jt=input('JobTracker IP=')
					tt1=input('TaskTracker1 IP=')
					tt2=input('TaskTracker2 IP=')
					client=input('Client IP=')
					z=open('/hosts.txt','w+')
					z.write("""127.0.0.1   localhost localhost.localdomain localhost4 localhost4.localdomain4
::1         localhost localhost.localdomain localhost6 localhost6.localdomain6

192.168.43.{}	nn.lw.com
192.168.43.{}	dn1.lw.com
192.168.43.{}	dn2.lw.com
192.168.43.{}	dn3.lw.com
192.168.43.{}	jt.lw.com
192.168.43.{}	tt1.lw.com
192.168.43.{}	tt2.lw.com
192.168.43.{}	client.lw.com""".format(nn,dn1,dn2,dn3,jt,tt1,tt2,client))
					sp.getoutput("scp /hosts.txt /etc/hosts")
				if int(func) == 2:
					sp.getoutput("scp /mapredcore.xml /etc/hadoop/core-site.xml")	
					sp.getoutput("scp /mapred.xml /etc/hadoop/mapred-site.xml")
					sp.getoutput("hadoop-daemon.sh start jobtracker")	
					x=sp.getstatusoutput("jps| grep JobTracker")
					if int(x[0])== 0:
						print("JobTracker Started Successfully")
					else:
						print("Failed to start JobTracker")
				if int(func) == 3:
					sp.getoutput("scp /mapredcore.xml /etc/hadoop/core-site.xml")	
					sp.getoutput("scp /blank.xml /etc/hadoop/hdfs-site.xml")
					sp.getoutput("scp /mapred.xml /etc/hadoop/mapred-site.xml")	
					sp.getoutput("hadoop-daemon.sh start tasktracker")	
					x=sp.getstatusoutput("jps| grep TaskTracker")
					if int(x[0])== 0:
						print("TaskTracker Started Successfully")
					else:
						print("Failed to start TaskTracker")
				print("Do You want to Continue MapRed Cluster Setup(y/n)? ",end='')
				choice=input()
				if choice == "YES" or choice == "Yes" or choice == "yes" or choice == "y":
					os.system("clear")
				elif choice == "NO" or choice == "No" or choice == "no" or choice == "n":
					j=0
					print("Now exiting")
		elif cl == "CLIENT" or cl == "Client" or cl == "client" or cl == "c" or int(cl)== 4:
			sp.getoutput("scp /core.xml /etc/hadoop/core-site.xml")
			sp.getoutput("scp /blank.xml /etc/hadoop/hdfs-site.xml")
			print("Hadoop Client is set for upload")
		elif cl == "EXIT" or cl == "Exit" or cl == "exit" or cl == "e" or cl == "5":
			print("Now Exiting")
			sp.getoutput("exit")
		else:
			print("You have selected an invalid option")
			print("Now exiting...")
			i=0
		print("Do You want to Continue Local Setup(y/n)? ",end='')
		choice=input()
		if choice == "YES" or choice == "Yes" or choice == "yes" or choice == "y":
			os.system("clear")
		elif choice == "NO" or choice == "No" or choice == "no" or choice == "n":
			i=0
			print("Now exiting")
elif loc == "remote" or loc == "Remote" or loc == "r" or loc == "R":
	print("Enter User's IP: 192.168.43.",end='')
	u_id=input()
	a=sp.getoutput("ssh-copy-id 192.168.43.{}".format(u_id))
	i=1
	while int(i)==1:
		print("""\nWhat do you want to setup? 
	1)HADOOP ENVIROMENT
	2)HDFS CLUSTER
	3)MAPRED CLUSTER
	4)CLIENT
	5)EXIT""")
		print("--------------------------------------------------------------------------------")
		print("Your Choice: ", end='')
		cl=input()
		if int(cl) == 1 or cl == "hadoop" or cl == "Hadoop" or cl == "HADOOP" :
			a=sp.getstatusoutput("ssh 192.168.43.{} rpm -q hadoop || rpm -ivh jdk".format(u_id))
			if int(a[0])== 0 :			
				print("Hadoop is already installed")
			else:
				sp.getoutput("rm -rf /etc/hadoop")
				sp.getstatusoutput("ssh 192.168.43.{} rpm -ivh /root/jdk-7u79-linux-x64.rpm".format(u_id))
				b=sp.getstatusoutput("ssh 192.168.43.{} rpm -ivh /root/hadoop-1.2.1-1.x86_64.rpm --force".format(u_id))
				if int(b[0]) == 0:
					print("Hadoop is Installed\nNow Setting Up Path")
				else:
					print("Hadoop not Installed")
				sp.getoutput("scp /copyrc 192.168.43.{}:/root/.bashrc".format(u_id))
				sp.getoutput("ssh 192.168.43.{} chmod +x /root/.bashrc".format(u_id))
				print("Hadoop has been Installed and Path is set ")
		elif cl == "HDFS" or cl == "Hdfs" or cl == "hdfs" or cl == "h" or int(cl)== 2:
			print("Enter Master's IP: 192.168.43." , end='')
			m_id=input('')
			j=1
			while int(j) == 1:
				print("""\n1)Setup NameNode
2)Setup DataNode""")
				print("--------------------------------------------------------------------------------")
				print("Your Choice: ", end='')
				func=input()
				if int(func) == 1:
					sp.getoutput("ssh 192.168.43.{} rm -rvf /master".format(u_id))
					sp.getoutput("scp /masterhdfs.xml 192.168.43.{}:/etc/hadoop/hdfs-site.xml".format(u_id))
					z=open("/core.xml",'w+')
					z.write("""<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>

<property>
<name>fs.default.name</name>
<value>hdfs://192.168.43.{}:9001</value>
</property>

</configuration>""".format(m_id))
					z.close()
					sp.getoutput("scp /core.xml 192.168.43.{}:/etc/hadoop/core-site.xml".format(u_id))
					sp.getoutput("ssh 192.168.43.{} hadoop namenode -format".format(u_id))
					sp.getoutput("ssh 192.168.43.{} hadoop-daemon.sh start namenode".format(u_id))
					time.sleep(2)
					x=sp.getstatusoutput("ssh 192.168.43.{} jps | grep NameNode".format(u_id))
					if int(x[0])== 0:
						print("Namenode Started successfully")
					else:
						print("Failed to start NameNode")
				elif int(func)==2:
					sp.getoutput("ssh 192.168.43.{} rm -rvf /data".format(u_id))
					sp.getoutput("scp /slavehdfs.xml 192.168.43.{}:/etc/hadoop/hdfs-site.xml".format(u_id))		
					z=open("/core.xml",'w+')
					z.write("""<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>

<property>
<name>fs.default.name</name>
<value>hdfs://192.168.43.{}:9001</value>
</property>

</configuration>""".format(m_id))
					z.close()
					sp.getoutput("scp /core.xml 192.168.43.{}:/etc/hadoop/core-site.xml".format(u_id))
					sp.getoutput("ssh 192.168.43.{} hadoop-daemon.sh start datanode".format(u_id))
					time.sleep(2)
					y=sp.getstatusoutput("ssh 192.168.43.{} jps| grep DataNode".format(u_id))
					if int(y[0])== 0:
						print("DataNode Started Successfully")
					else:
						print("Failed to start DataNode")
				else:
					print("Invalid option, Now Exiting...")
					i=0
					j=0
				print("Do You want to Continue HDFS Cluster Setup(y/n)? ",end='')
				choice=input()
				if choice == "YES" or choice == "Yes" or choice == "yes" or choice == "y":
					os.system("clear")
					j=1
				elif choice == "NO" or choice == "No" or choice == "no" or choice == "n":
					j=0
					print("Now exiting")
		elif cl == "MAPRED" or cl == "Mapred" or cl == "mapred" or cl == "m" or int(cl)== 3:
			k=1
			while int(k) == 1:
				print("""______________________________________________________________________________
\t-----------------WARNING-------------
________________________________________________________________________________
Make Sure Your /etc/hosts file is configured in the following manner:- \n\n
127.0.0.1   localhost localhost.localdomain localhost4 localhost4.localdomain4
::1         localhost localhost.localdomain localhost6 localhost6.localdomain6

NameNode's IP		nn.lw.com
DataNode1's IP		dn1.lw.com
DataNode2's IP		dn2.lw.com
DataNode3's IP		dn3.lw.com
JobTracker's IP		jt.lw.com
TaskTracker2's IP	tt1.lw.com
TaskTracker1's IP	tt2.lw.com
Client's IP		client.lw.com
________________________________________________________________________________""")
				time.sleep(1)
				print("""1)Setup Hosts File
2)Setup JobTracker
3)Setup TaskTracker""")
				print("--------------------------------------------------------------------------------")
				print("Your Choice: ",end='')
				func = input()
				if int(func) == 1:
					nn=input('NameNode IP= 192.168.43.')
					dn1=input('Datanode1 IP=192.168.43.')
					dn2=input('Datanode2 IP=192.168.43.')
					dn3=input('Datanode3 IP=192.168.43.')
					jt=input('JobTracker IP=192.168.43.')
					tt1=input('TaskTracker1 IP=192.168.43.')
					tt2=input('TaskTracker2 IP=192.168.43.')
					client=input('Client IP=192.168.43.')
					z=open('/hosts.txt','w+')
					z.write("""127.0.0.1   localhost localhost.localdomain localhost4 localhost4.localdomain4
::1         localhost localhost.localdomain localhost6 localhost6.localdomain6

192.168.43.{}	nn.lw.com
192.168.43.{}	dn1.lw.com
192.168.43.{}	dn2.lw.com
192.168.43.{}	dn3.lw.com
192.168.43.{}	jt.lw.com
192.168.43.{}	tt1.lw.com
192.168.43.{}	tt2.lw.com
192.168.43.{}	client.lw.com """.format(nn,dn1,dn2,dn3,jt,tt1,tt2,client))
					z.close()
					sp.getoutput("scp /hosts.txt 192.168.43.{}:/etc/hosts".format(u_id))
				elif int(func) == 2:
					sp.getoutput("scp /mapredcore.xml 192.168.43.{}:/etc/hadoop/core-site.xml".format(u_id))	
					sp.getoutput("scp /blank.xml 192.168.43.{}:/etc/hadoop/hdfs-site.xml".format(u_id))
					sp.getoutput("scp /mapred.xml 192.168.43.{}:/etc/hadoop/mapred-site.xml".format(u_id))	
					sp.getoutput("scp /hosts.txt 192.168.43.{}:/etc/hosts")
					sp.getoutput("ssh 192.168.43.{} hadoop-daemon.sh start jobtracker".format(u_id))	
					time.sleep(2)
					x=sp.getstatusoutput("ssh 192.168.43.{} jps| grep JobTracker".format(u_id))
					if int(x[0])== 0:
						print("JobTracker Started Successfully")
					else:
						print("Failed to start JobTracker")
				elif int(func) == 3:
					sp.getoutput("scp /mapredcore.xml 192.168.43.{}:/etc/hadoop/core-site.xml".format(u_id))	
					sp.getoutput("scp /blank.xml 192.168.43.{}:/etc/hadoop/hdfs-site.xml".format(u_id))
					sp.getoutput("scp /mapred.xml 192.168.43.{}:/etc/hadoop/mapred-site.xml".format(u_id))	
					sp.getoutput("scp /hosts.txt 192.168.43.{}:/etc/hosts")
					sp.getoutput("ssh 192.168.43.{} hadoop-daemon.sh start tasktracker".format(u_id))	
					x=sp.getstatusoutput("ssh 192.168.43.{} jps| grep TaskTracker".format(u_id))
					if int(x[0])== 0:
						print("TaskTracker Started Successfully")
					else:
						print("Failed to start TaskTracker")
				else:
					print("Invalid option, Now Exiting...")
					k=0
				print("Do You want to Continue MAPRED Cluster Setup(y/n)? ",end='')
				choice=input()
				if choice == "YES" or choice == "Yes" or choice == "yes" or choice == "y":
					os.system("clear")
					k=1
				elif choice == "NO" or choice == "No" or choice == "no" or choice == "n":
					k=0
					print("Now exiting")
		
		elif cl == "CLIENT" or cl == "Client" or cl == "client" or cl == "c" or int(cl)== 4:
			sp.getoutput("scp /core.xml 192.168.43.{}:/etc/hadoop/core-site.xml".format(u_id))
			sp.getoutput("scp /blank.xml 192.168.43.{}:/etc/hadoop/hdfs-site.xml".format(u_id))
			print("Hadoop Client is set for upload")
		elif cl == "EXIT" or cl == "Exit" or cl == "exit" or cl == "e" or cl == "5":
			print("Now Exiting")
			sp.getoutput("exit")		
		else:
			print("You have selected an invalid option")
			print("Now exiting...")
			i=0
		print("Do You want to Continue Remote Setup(y/n)? ",end='')
		choice=input()
		if choice == "YES" or choice == "Yes" or choice == "yes" or choice == "y":
			os.system("clear")
			i=1
		elif choice == "NO" or choice == "No" or choice == "no" or choice == "n":
			i=0
			print("Now exiting")
else:
	print("You have selected an invalid option")
	print("Now exiting...")
