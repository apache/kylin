# kylin-test
Automated test code repo based on [gauge](https://docs.gauge.org/?os=macos&language=python&ide=vscode) for [Apache Kylin](https://github.com/apache/kylin).

### IDE
Gauge support IntelliJ IDEA and VSCode as development IDE.
However, IDEA cannot detect the step implementation method of Python language, just support java.
VSCode is recommended as the development IDE.

### Clone repo
```
git clone https://github.com/zhangayqian/kylin-test
```

### Prepare environment
 * Install python3 compiler and version 3.6 recommended
 * Install gauge
 ```
 brew install gauge
 ```
 If you encounter the below error:
 ```
 Download failed: https://homebrew.bintray.com/bottles/gauge- 1.1.1.mojave.bottle.1.tar.gz
 ```
 You can try to download the compressed package manually, put it in the downloads directory of homebrew cache directory, and execute the installation command of gauge again.

* Install required dependencies
```
pip install -r requirements.txt
```

## Directory structure
* features/specs: Directory of specification file.
  A specification is a business test case which describes a particular feature of the application that needs testing. Gauge specifications support a .spec or .md file format and these specifications are written in a syntax similar to Markdown.
  
* features/step_impl: Directory of Step implementations methods.
  Every step implementation has an equivalent code as per the language plugin used while installing Gauge. The code is run when the steps inside a spec are executed. The code must have the same number of parameters as mentioned in the step.
  Steps can be implemented in different ways such as simple step, step with table, step alias, and enum data type used as step parameters.

* data: Directory of data files needed to execute test cases. Such as cube_desc.json.

* env/default: Gauge configuration file directory.

* kylin_instance: Kylin instance configuration file directory.

* kylin_utils: Tools method directory.

## Run Gauge specifications
* Run all specification
```
gauge run
```
* Run specification or step or spec according tags, such as:
```
gauge run --tags 3.x
```
* Please refer to https://docs.gauge.org/execution.html?os=macos&language=python&ide=vscode learn more.

## Tips

A specification consists of different sections; some of which are mandatory and few are optional. The components of a specification are listed as follows:

- Specification heading
- Scenario
- Step
- Parameters
- Tags
- Comments

#### Note

Tags - optional, executable component when the specification is run
Comments - optional, non-executable component when the specification is run

### About tags

Here, we stipulate that all test scenarios should have tags. Mandatory tags include 3.x and 4.x to indicate which versions are supported by the test scenario. Such as:
```
# Flink Engine
Tags:3.x
```
```
# Cube management
Tags:3.x,4.x
```
You can put the tag in the specification heading, so that all scenarios in this specification will have this tag.
You can also tag your own test spec to make it easier for you to run your own test cases.

### About Project
There are two project names already occupied, they are `generic_test_project` and `pushdown_test_project`. 
  
Every time you run this test, @befroe_suit method will be execute in advance to create `generic_test_project`.  And the model and cube in this project are universal, and the cube has been fully built. They include dimensions and measures as much as possible. When you need to use a built cube to perform tests, you may use it.

`pushdown_test_project` used to compare sql query result. This is a empty project.

Please refer to https://docs.gauge.org/writing-specifications.html?os=macos&language=python&ide=vscode learn more.
