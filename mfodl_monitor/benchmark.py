import os
import time

# add your paths
path_to_mfodl = "/Users/krq770/CLionProjects/timelymon/mfodl_monitor"
path_to_monpoly = "/Users/krq770/Desktop/Experiments_Stream_Monitor/monpoly"
path_to_data = "/Users/krq770/CLionProjects/timelymon/mfodl_monitor/data/"

runs = 10
numbers_worker = [1,2,4,6]

data_timely = {
"in_order": ["50K50TS.csv", "100K100TS.csv",	"250K250TS.csv"],	
"reverse": ["reverse_50K50TS.csv", "reverse_100K100TS.csv", "reverse_250K250TS.csv"],		
"reorder_epoch": ["reorder_epoch_50K50TS.csv", "reorder_epoch_100K100TS.csv", "reorder_epoch_250K250TS.csv"],
"reorder_tuple": ["reorder_tuple_50K50TS.csv", "reorder_tuple_100K100TS.csv",	"reorder_tuple_250K250TS.csv"],

"2md5sd": ["50K2md5sd_out_of_order.csv", "100K2md5sd_out_of_order.csv", "250K2md5sd_out_of_order.csv"],
"2md15sd": ["50K2md15sd_out_of_order.csv", "100K2md15sd_out_of_order.csv", "250K2md15sd_out_of_order.csv"],
"2md25sd": ["50K2md25sd_out_of_order.csv", "100K2md25sd_out_of_order.csv", "250K2md25sd_out_of_order.csv"],	

"5md5sd": ["50K5md5sd_out_of_order.csv", "100K5md5sd_out_of_order.csv", "250K5md5sd_out_of_order.csv"],
"5md15sd": ["50K5md15sd_out_of_order.csv", "100K5md15sd_out_of_order.csv", "250K5md15sd_out_of_order.csv"],
"5md25sd": ["50K5md25sd_out_of_order.csv", "100K5md25sd_out_of_order.csv", "250K5md25sd_out_of_order.csv"]
}

data_monpoly = [ "verimon_50K", "verimon_100K", "verimon_250K"]

res_timely = {}
res_monpoly = {}



# TimelyMon
os.chdir(path_to_mfodl)
# todo call build_data_script

for (name, formula) in [("Negated_Triangle", "((Once[0,7] A(a,b)) && B(b,c)) && (~ Eventually[0,7] C(c,a))"), ("Triangle", "((Once[0,7] A(a,b)) && B(b,c)) && (Eventually[0,7] C(c,a))"), ("Linear", "((Once[0,7] A(a,b)) && B(b,c)) && Eventually[0,7] C(c,d)"), ("Star", "((Once[0,7] A(a,b)) && B(a,c)) && Eventually[0,7] C(a,d)")]:
	for (key, data) in data_timely.items():
		print(key)
		time_per_file = []
		for (d, size) in zip(data, [50,100,250]):
			time_per_worker = []
			for nm in numbers_worker:
				times = []
				for i in range(0, runs):
					start = time.time()
					os.system("./target/release/timelymon " + "\"" + formula + "\"" + " data/" + name + "/" + d + " -m 2 -w " + str(nm))
					res = time.time() - start
					print(res)
					times.append(res)
				time_per_worker.append((nm, sum(times)/int(runs)))
			time_per_file.append((size, time_per_worker))
		res_timely[name + " " + key] = time_per_file


# monpoly/verimon
options = [("Negated_Triangle", "((ONCE[0,7] A(a,b)) AND B(b,c)) AND (NOT EVENTUALLY[0,7] C(c,a))"), ("Triangle", "((ONCE[0,7] A(a,b)) AND B(b,c)) AND (EVENTUALLY[0,7] C(c,a))"), ("Linear", "((ONCE[0,7] A(a,b)) AND B(b,c)) AND EVENTUALLY[0,7] C(c,d)"), ("Star", "((ONCE[0,7] A(a,b)) AND B(a,c)) AND EVENTUALLY[0,7] C(a,d)")]
os.chdir(path_to_monpoly)
#write sig file
if not os.path.exists("Benchmark.sig"):
	with open("Benchmark.sig", "w") as f:
		f.write("A(int,int)\nB(int,int)\nC(int,int)")
		f.close()

reload_options = []
for (x,x1) in options:
	if not os.path.exists(x1 + "_formula.txt"):
		reload_options.append((x,x1))
#write formula files
for (name, formula) in reload_options:
	with open(name + "_formula.txt", "w") as f:
		f.write(formula)
		f.close()

for (name, _f) in options:
	for mode in ["verimon", "monpoly"]:
		time_per_file = []
		for (d, size) in zip(data_monpoly, [50,100,250]):
			times = []
			for i in range(0, runs):
				start = time.time()
				if mode == "verimon":
					os.system("./monpoly -sig Benchmark.sig -formula " + name + "_formula.txt -verified " + "-log " + path_to_data + "/" + name + "/" + d + " > trash.txt") # + "\"" + formula + "\" " + "data/" + name + "/" + d + " -w" + str(nm))
				else:
					os.system("./monpoly -sig Benchmark.sig -formula " + name + "_formula.txt" + " -log " + path_to_data + "/" + name + "/" + d + " > trash.txt")
				res = time.time() - start
				print(res)
				times.append(res)
			time_per_file.append((size, sum(times)/int(runs)))
		res_monpoly[name + " " + mode] = time_per_file

def print_result(mode):
	res = res_monpoly
	if mode == "timelymon":
		print("-----------TimelyMon")
		res = res_timely
	else:
		print("-----------MonPoly/VeriMon")

	for (name, time_per_file) in res.items():
		print("------" + name)
		for (n, tpw) in time_per_file:
			print(str(n) + ":   " + str(tpw))
	print("\n")


def convert_list_to_coordinates(res_list):
	res = ""
	for tup in res_list:
		res += str(tup) + " "
	return res

def extract_data(setting, formula, tps):
	if setting == "monpoly" or setting == "verimon":
		for (n, tpw) in res_monpoly[formula + " " + setting]:
			if n == tps:
				return [(1, tpw)]
	else:
		for (n, tpw) in res_timely[formula + " " + setting]:
			if n == tps:
				return tpw
	return []

def build_subfigure(formula, tps):
	settings = ["in_order", "reverse", "reorder_epoch", "reorder_tuple", "2md5sd", "2md15sd", "2md25sd", "5md5sd", "5md15sd", "5md25sd", "monpoly", "verimon"]
	colormarks = ["blue,mark=-,", "orange,mark=|,", "teal,mark=pentagon,", "red,mark=pentagon*,", "purple,mark=oplus,", "brown,mark=10-pointed star,", "pink,mark=otimes,", "gray,mark=otimes*,", "olive,mark=halfcircle,", "cyan,mark=halfcircle*,", "blue,mark=square,", "orange,mark=square*,"]

	subfigure = "\\begin{subfigure}[t]{.5\\textwidth}\n \\centering \n \\begin{tikzpicture}[scale = \\plotscale] \n \\begin{axis}[xlabel = Number of Workers, ylabel = Time in Seconds, y post scale=0.70]\n"
	for (setting, colormark) in zip(settings, colormarks):
		vals = extract_data(setting, formula, tps)
		subfigure += "\\addplot [domain=" + str(numbers_worker[0]) + ":" + str(numbers_worker[len(numbers_worker)-1]) + ", color=" + colormark + "] coordinates {" + convert_list_to_coordinates(vals) + "};\n"
	subfigure += "\\end{axis} \n \\end{tikzpicture} \n \\caption{" + formula + "; " + str(tps) + " time-points} \n \\label{fig:" + formula + str(tps) + "} \n \\end{subfigure}%"
	
	os.chdir(path_to_mfodl)
	os.system("mkdir result_subfigure_1")
	os.chdir("result_subfigure_1")

	with open(formula + "" + str(tps) + ".txt", "w") as f:
		f.write(subfigure)
		f.close()

	return subfigure

print_result("timelymon")


print(build_subfigure("Negated_Triangle", 50))
print(build_subfigure("Negated_Triangle", 100))
print(build_subfigure("Negated_Triangle", 250))

print(build_subfigure("Linear", 50))
print(build_subfigure("Linear", 100))
print(build_subfigure("Linear", 250))

print(build_subfigure("Triangle", 50))
print(build_subfigure("Triangle", 100))
print(build_subfigure("Triangle", 250))

print(build_subfigure("Star", 50))
print(build_subfigure("Star", 100))
print(build_subfigure("Star", 250))
