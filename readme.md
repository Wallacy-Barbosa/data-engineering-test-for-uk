## Data Engineer Case

### Disclaimer
#### * This project was created using scala, scalatest, spark and maven
#### * Two objects were created:
    Main (object): Starts the process and does the dependency injection of the arguments and SparkSession
    BusinessRuleProcessor (class): Process all business rule

#### 3. Tests

In the resource folder, there are the files with the mass of the input and output tests: 

    input_file_path
        file1.csv
        file2.tsv
    
    output_file_path
        file1.tsv
        file2.tsv

Tests details:

** The tests are part of the documentation, that's why, they were created using BDD.
<br>
** Tests were created considering end to end execution/separate functions, as well as expected success and failure scenarios.
<br>
** This application follows the test coverage recommendations above 80%

Tests Components:

    BeforeAll is excuted before all tests
    AfterAll is excuted after all tests

    MainTest

        Test 01: When running the project successfully, it
            This test is responsible for running the end-to-end process and ensuring that the output file is correct.

        Test 02: when checking parameters, it
            This test is responsable for check to arguments of application

    BusinessRuleProcessorTest

        Test 03: when checking extension of input file, it
            This test is responsible for ensuring that the input file have the correct extension.

#### 4. How to execute the process:
    We recommend running the process through the intelliJ IDE

    5.1. Download the maven libraries
    5.2. Uncomment the lines 41, 42, 43 e 44 from Main object
    5.3. Execute Main object
