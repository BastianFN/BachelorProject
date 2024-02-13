# TimelyMon
### Mfotl
As introduced in the [paper](https://people.inf.ethz.ch/ioannisl/mfodl.pdf).
### Timely Dataflow
Add paper
### Streaming Monitor
Add paper 


## How to use
### Build from Git
* Clone repository
* Naviagte to the ```mfodl_monitor``` folder
* Select the ```Cargo.toml``` file
* Run ```cargo build -r```

### Build from archive
* Unzip the archive
* Naviagte to the ```mfodl_monitor``` folder
* Select the ```Cargo.toml``` file
* Run ```cargo build -r```

### Run the monitor
The monitor can be accessed in different ways including:
```bash
./target/release/timelymon
```
Or:
```
cargo run --bin timelymon
```
### Options and Flags
* Policy to enforce  
```"(Once[0,10]A(a,b) && B(b,c) && Eventually[0,10]C(c,d))"```
* Path to data set  
```data/Linear/50K50TS.csv```
* (optional) Number of Worker [default 1]  
```-w 6```
* (optional) Output mode [default 0]  
```-m 1```
  * 0: write to file (result will be sorted)
  * 1: print to stdout (potentially out-of-order)
  * 2: result is discarded
* (optional) Provided Output file  
```-o linear_out.txt```
* (optional) Number of tuples to ingest before doing a work Step [default 1000]  
```-s 10000```
* (optional) Deduplication for specified operators [default false]  
```-d```

### Offline and Online Monitoring
The monitor can be used for offline monitoring (working on already complete data sets) and online
monitoring (data is continuously streamed). For offline monitoring the data set is provided as a flag to the monitor,
and for online monitoring the data source is piped into the monitor. 

## Policies
### Facts
Facts can have one or multiple parameters of the following types:
 * String

```bash
> p('foo')
Fact("p", ["foo"])
```
 
 * Integer

```bash
> r(4)
Fact("r", ["4"])
```

 * Variable names

```
> q(x)
Fact("q", [x])
```

 * Multiple parameters

```bash
> p(1, 2, 3)
Fact("p", ["1", "2", "3"])
> p('hello', 'world')
Fact("p", ["hello", "world"])
> p(1, 'hello', x)
Fact("p", ["1", "hello", "x"])
```

#### not

```bash
> ~p(4)
Not(Fact("p", ["4"]))
```

#### conjunction

```bash
> p(4) && r(5)
Conj(Fact("p", ["4"]), Fact("r", ["5"]))
```

#### exists

```bash
> Ex.p(4)
Exists("x", Fact("p", ["4"]))
```
The precedence of the exists and forall operators requires some clarification:
  - ```Ex.p(4) && p(5)``` is equivalent to ```(Ex.p(4)) && p(5)```
  - if you want the exists operator to span across the and operator as well, you will need to put brackets: ``` Ex. ( p(4) && p(5) ) ```
  

#### temporal

```bash
A(x) Since [0,10] B(x,y)
A(x) Until [0,10] B(x,y)

Once [0,10] B(x,y)
Eventually [0,10] B(x,y)

Historically [0,10] B(x,y)
Always [0,10] B(x,y)

```

### Additional operators

These operators are outside the minimal language of MFODL, but can be expressed in terms of the minimal language specified above. The parser understands them and transforms them into their equivallent formulas using just the minimal language. As an example:

```bash
> p(4) || p(5)
Not(Conj(Not(Fact("p", ["4"])), Not(Fact("p", ["5"]))))
```

As with the temporal operator, all operators here will take the default time frame when one is not specified.

#### disj

```bash
> p(4) || p(5)
Not(Conj(Not(Fact("p", ["4"])), Not(Fact("p", ["5"]))))
```

#### implication

```bash
> p(4) -> p(5)
Not(Conj(Fact("p", ["4"]), Not(Fact("p", ["5"]))))
```

#### if and only if

```bash
> p(4) <-> p(5)
Conj(Not(Conj(Fact("p", ["4"]), Not(Fact("p", ["5"])))), Not(Conj(Fact("p", ["5"]), Not(Fact("p", ["4"])))))
```

#### forall

```bash
> Ax.p(4)
Not(Exists("x", Not(Fact("p", ["4"]))))
```

The precedence of the exists and forall operators requires some clarification:
  - ```Ex.p(4) && p(5)``` is equivalent to ```(Ex.p(4)) && p(5)```
  - if you want the exists operator to span across the and operator as well, you will need to put brackets: ``` Ex. ( p(4) && p(5) ) ```

### Operator precedence
From higher to lower precedence, the operators go like this:

* Formulas  

and > not = exists > temporal = fact

By default, binary operators like ```&&```, ```+``` and ```.``` are right-associative, which means that ```p(4)&&p(4)&&p(4)``` will be parsed as ```p(4)&&(p(4)&&p(4))```.
Parentheses take precedence over all other rules.

### Formal grammar
The grammar that the parser uses is as follows:

formula     := iff  
iff         := implication '<->' implication  
implication := conj {'->' conj}  
conj        := disj {'&&' conj}  
disj        := possibly {'||' disj}  
since       := until {'<S.>' until}  
until       := formula_l1 {'<U.>' formula_l1}  

formula_l1  := '~' formula_l1 | formula_l2  
formula_l2  := once | always | historically |  
eventually | p(arg+) | '(' formula ')' |   
'E' var '.' formula_l1 | 'A' var '.' formula_l1

## Testing
### Static test cases
The project contains around 150 unit test that can be run with ``` cargo test -r ```. 
### End-to-End
The project is shipped with a rudimentary end-to-end testing suit,
that executes 4 different formulas each on 3 data set sizes with 10 different settings. The expected result for comparison are produced by VeriMon.
Run with ```./target/release/verify``` or ```cargo run --bin verify```
