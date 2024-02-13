import os
import random

# change paths
path_to_mfodl = "/Users/krq770/CLionProjects/timelymon/mfodl_monitor"
path_to_data = "/Users/krq770/CLionProjects/timelymon/mfodl_monitor/data/"
path_to_monpoly = "/Users/krq770/Desktop/Experiments_Stream_Monitor/monpoly"


def parse_tp(line):
  line.strip()
  lines = line.split(",")
  tp = lines[1].split("=")
  return int(tp[1])

def write_file(data, name, file_name):
	with open(name + file_name, "w") as f:
		for d in data:
			f.write(d)

def reverse(file, file_name):
	file.reverse()
	write_file(file, "reverse_", file_name)
	

def reorder_epoch(file, file_name):
	segment_tp = parse_tp(file[0])
	segements = []

	current_segments = []
	for line in file:
		if segment_tp == parse_tp(line):
			current_segments.append(line)
		else:
			segements.append(current_segments)
			current_segments = []
			segment_tp = parse_tp(line)
			current_segments.append(line)
	segements.append(current_segments)
	random.shuffle(segements)

	res = []
	for seg in segements:
		for s in seg:
			res.append(s)

	write_file(res, "reorder_epoch_", file_name)

def reorder_tuple(file, file_name):
	random.shuffle(file)
	write_file(file, "reorder_tuple_", file_name)


# create folder structure
print("-----------Create folder structure:")
os.chdir(path_to_mfodl)
if not os.path.isdir("data"):
	os.system("mkdir data")
	os.chdir(path_to_data)
	os.system("mkdir Linear")
	os.system("mkdir Star")
	os.system("mkdir Triangle")
	os.system("mkdir Negated_Triangle")
os.chdir(path_to_data)
print("-----------Folder structure created")


print("-----------Create MonPoly Settings:")
options = [("Triangle", "((ONCE[0,7] A(a,b)) AND B(b,c)) AND (EVENTUALLY[0,7] C(c,a))"), ("Negated_Triangle", "((ONCE[0,7] A(a,b)) AND B(b,c)) AND (NOT EVENTUALLY[0,7] C(c,a))"), ("Linear", "((ONCE[0,7] A(a,b)) AND B(b,c)) AND EVENTUALLY[0,7] C(c,d)"), ("Star", "((ONCE[0,7] A(a,b)) AND B(a,c)) AND EVENTUALLY[0,7] C(a,d)")]
os.chdir(path_to_monpoly)
#write sig file
if not os.path.exists("Benchmark.sig"):
	with open("Benchmark.sig", "w") as f:
		f.write("A(int,int)\nB(int,int)\nC(int,int)")
		f.close()

reload_options = []
for (x,x1) in options:
	if not os.path.exists(x + "_formula.txt"):
		reload_options.append((x,x1))
#write formula files
for (name, formula) in reload_options:
	with open(name + "_formula.txt", "w") as f:
		f.write(formula)
		f.close()
print("-----------MonPoly Settings created")


# create base sets
print("-----------Create Base data sets:")
for (formula, directory, form) in [("T", "Triangle", "((Once[0,7] A(a,b)) && B(b,c)) && (Eventually[0,7] C(c,a))"), ("T", "Negated_Triangle", "((Once[0,7] A(a,b)) && B(b,c)) && (~ Eventually[0,7] C(c,a))"), ("L", "Linear", "((Once[0,7] A(a,b)) && B(b,c)) && Eventually[0,7] C(c,d)"), ("S", "Star", "((Once[0,7] A(a,b)) && B(a,c)) && Eventually[0,7] C(a,d)")]:
	os.chdir(path_to_data + directory)

	with open(directory + "_Formula.txt", "w") as f:
		f.write(form)
		f.close()

	docker = "docker run -iv " + path_to_data + directory
	dir_= ":/work infsec/benchmark generator -" + formula
	basic = " -pA 0.1 -pB 0.5 -z \"x=1.5+3,z=2\" -x 10 -e 1000 "
	base_com = docker + dir_ + basic

	for set_size in ["50","100","250"]:
		file_name = set_size + "K" + set_size + "TS.csv"
		rest = set_size + " > " + file_name
		if not os.path.exists(file_name):
			os.system(base_com + rest)

		new_file_content = []
		for line in open(file_name):
  			new_file_content.append(line)

		if not os.path.exists("verimon_" + set_size + "K"):
  			os.system(docker + ":/work infsec/benchmark replayer -f verimon -a 0 < " + file_name + " > verimon_" + set_size + "K")
		if not os.path.exists("result_" + set_size + "K"):
			os.chdir(path_to_monpoly)
			os.system("./monpoly -sig Benchmark.sig -formula " + directory + "_formula.txt -ignore_parse_errors -log " + path_to_data + directory + "/verimon_" + set_size + "K > " + path_to_data + directory + "/result_" + set_size + "K")
			os.chdir(path_to_data + directory)
		if not os.path.exists("reverse_" + file_name):
			reverse(new_file_content, file_name)
		if not os.path.exists("reorder_epoch_" + file_name):
			reorder_epoch(new_file_content, file_name)
		if not os.path.exists("reorder_tuple_" + file_name):
			reorder_tuple(new_file_content, file_name)
		if not os.path.exists(file_name):
			new_file_content = []
			for line in open(file_name):
  				new_file_content.append(line)

			segment_tp = parse_tp(new_file_content[0])
			segements = []
			for line in new_file_content:
  				if segment_tp == parse_tp(line):
  					segements.append(line)
  				else:
  					segements.append(">WATERMARK " + str(segment_tp) + "<\n")
  					segment_tp = parse_tp(line)
  					segements.append(line)
  
			out = open(file_name, "w")
			out.writelines(segements)
print("-----------Base data sets created")


# create delayed data
print("-----------Create dalyed data:")
counter = 0
for (formula, directory) in [("T", "Triangle"), ("T", "Negated_Triangle"), ("L", "Linear"), ("S", "Star")]:
	os.chdir(path_to_data + directory)
	docker = "docker run -iv " + path_to_data + directory
	dir_ = ":/work infsec/benchmark generator -" + formula
	basic = " -pA 0.1 -pB 0.5 -z \"x=1.5+3,z=2\" -x 10 -et -e 1000 "
	base_com = docker + dir_ + basic

	for set_size in ["50","100","250"]:
		for md in ["2","5"]:
			for sd in ["5", "15", "25"]:
				file_name = set_size + "K" + md + "md" + sd + "sd_out_of_order.csv"
				md_sd =  "-md " + md + " -s " + sd + " -wp 1 " + set_size + " > " + file_name
				if os.path.exists(file_name) and os.path.getsize(path_to_data + directory + "/" + file_name) <= 0:
					os.system(base_com + md_sd)
				if not os.path.exists(file_name):
					#print(base_com + md_sd)
					os.system(base_com + md_sd)
				counter += 1
				print("Dataset " + str(counter) + " of 72")
print("-----------Delayed data created")

print("Script finished")
