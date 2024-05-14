import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as redshiftserverless from 'aws-cdk-lib/aws-redshiftserverless';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';

import { PythonFunction } from '@aws-cdk/aws-lambda-python-alpha';

export interface RedshiftProps {
  sourceBucket: s3.Bucket;
}

export class Redshift extends Construct {
  public readonly workgroupArn: string;
  public readonly workgroupName: string;
  public readonly namespace: string;
  public readonly database: string;
  public readonly rsRole: iam.Role;
  public readonly lambdaRole: iam.Role;

  constructor(scope: Construct, id: string, props: RedshiftProps) {
    super(scope, id);

    const projectname = 'analysissystem'; // used for workgroup, namespace, db name, etc
    this.workgroupName = projectname;
    this.database = projectname;
    this.namespace = projectname;

    this.lambdaRole = new iam.Role(this, 'LambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com')
    });
    new PythonFunction(this, 'Init', {
      runtime: lambda.Runtime.PYTHON_3_12,
      entry: 'lambda/redshiftinit',
      handler: 'handler',
      memorySize: 256,
      timeout: cdk.Duration.minutes(15),
      environment: {
        REDSHIFT_WORKGROUP: this.workgroupName,
        REDSHIFT_NAMESPACE: this.namespace,
        REDSHIFT_DATABASENAME: this.database
      },
      role: this.lambdaRole
    });
    
    // role for the Redshift to read csv from the source s3 bucket.
    this.rsRole = new iam.Role(this, 'Role', {
      assumedBy: new iam.ServicePrincipal('redshift.amazonaws.com')
    });
    props.sourceBucket.grantRead(this.rsRole);

    // RedShift Credentials
    const secret = new secretsmanager.Secret(this, 'Secret', {
      generateSecretString: {
        secretStringTemplate: JSON.stringify({ username: 'admin' }),
        generateStringKey: 'password',
        excludeCharacters: '\'"/@ \\'
      }
    });

    // Redshift Serverless namespace
    const ns = new redshiftserverless.CfnNamespace(this, 'Namespace', {
      namespaceName: this.namespace,
      dbName: this.database,
      defaultIamRoleArn: this.rsRole.roleArn,
      adminUsername: 'admin',
      adminUserPassword: secret.secretValueFromJson('password').unsafeUnwrap(), // safe usage.
      iamRoles: [this.rsRole.roleArn]
    });

    // Redshift Serverless workgroup
    const workGroup = new redshiftserverless.CfnWorkgroup(this, 'Workgroup', {
      namespaceName: this.namespace,
      workgroupName: this.workgroupName
    });
    this.workgroupArn = workGroup.attrWorkgroupWorkgroupArn;
    workGroup.addDependency(ns);

    this.lambdaRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'));
    this.lambdaRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['redshift-data:GetStatementResult', 'redshift-data:DescribeStatement'],
        resources: ['*']
      })
    );
    this.lambdaRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['redshift-serverless:GetCredentials', 'redshift-data:BatchExecuteStatement', 'redshift-data:ExecuteStatement'],
        resources: [workGroup.attrWorkgroupWorkgroupArn]
      })
    );
    props.sourceBucket.grantRead(this.lambdaRole)


  }
}
