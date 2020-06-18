# Java Samples for Rapid Prototyping

This is a project to experience application development on AWS with the actual minimal implementations!

It contains:
* Basic 3-tiers serverlss WEB application which depends on:
    * Amazon API Gateway
    * AWS Lambda
    * Amazon DynamoDB

## Prerequisite
* Java 8 or Later
* Maven 3.6.1 or Later
* CDK 1.31.0

## deploy
1. `cd lambda`
2. `mvn install` package lambda source files into jar and install to maven local repository
3. `cd ../cdk`
4. `mvn compile`     compile cdk source files
5. `cdk bootstrap`   deploy bootstrap stack (If you deploy first at region)
6. `cdk deploy`      deploy resources via cdk

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

