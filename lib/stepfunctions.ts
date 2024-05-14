import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as sfntasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as scheduler from 'aws-cdk-lib/aws-scheduler';
import { Redshift } from './redshift';
import { PythonFunction } from '@aws-cdk/aws-lambda-python-alpha';

export interface WorkFlowProps {
  sourceBucket: s3.Bucket;
  vpc: ec2.IVpc;
  dbName: string;
  dbPort: string;
  dbHostname: string;
  dbSecret: cdk.aws_secretsmanager.ISecret;
}

export class WorkFlow extends Construct {
  private rs: Redshift;
  

  constructor(scope: Construct, id: string, props: WorkFlowProps) {
    super(scope, id);

    this.rs = new Redshift(this, 'Redshift', { sourceBucket: props.sourceBucket});
    
    //1st query to Redshift to avoid internal error
    const setuprs = new PythonFunction(this, 'SetUpRedshift', {
      runtime: lambda.Runtime.PYTHON_3_12,
      entry: 'lambda/redshiftsetup',
      handler: 'handler',
      memorySize: 256,
      timeout: cdk.Duration.minutes(15),
      environment: {
        REDSHIFT_WORKGROUP: this.rs.workgroupArn,
        REDSHIFT_DATABASENAME: this.rs.database
      },
      role: this.rs.lambdaRole
    });
    
    const setupkicker = new sfntasks.LambdaInvoke(this, 'SfnSetUpRedshift', {
      lambdaFunction: setuprs,
      resultPath: '$.RDSInfo'
    });
    
    //waitTime
    const wait = new sfn.Wait(this, 'Wait', {
      time: sfn.WaitTime.secondsPath('$.waitSeconds'),
    });

    const rdsKickerLambdaRole = new iam.Role(this, 'LambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com')
    });
    rdsKickerLambdaRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'));
    rdsKickerLambdaRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'));
    props.dbSecret.grantRead(rdsKickerLambdaRole)

    //Lambda function that export Aurora table as CSV in S3 bucket
    const rdskicker = new PythonFunction(this, 'RdsLoadKicker', {
      runtime: lambda.Runtime.PYTHON_3_12,
      entry: 'lambda/auroraloadkicker',
      handler: 'handler',
      memorySize: 256,
      timeout: cdk.Duration.minutes(15),
      vpc: props.vpc,
      environment: {
        DB_HOSTNAME: props.dbHostname,
        DB_NAME: props.dbName,
        DB_SECRET_NAME: props.dbSecret.secretName,
        DATASOURCE_BUCKET_NAME: props.sourceBucket.bucketName
      },
      role:rdsKickerLambdaRole
    });
    
    //invoke lambda from Step Function
    const rdsloadkicker = new sfntasks.LambdaInvoke(this, 'SfnAuroraToS3', {
      lambdaFunction: rdskicker,
      resultPath: '$.RDSInfo'
    });

    const s3checker = new PythonFunction(this, 'S3Check', {
      runtime: lambda.Runtime.PYTHON_3_12,
      entry: 'lambda/checks3object',
      handler: 'handler',
      memorySize: 128,
      timeout: cdk.Duration.minutes(15),
      environment: {
        DATASOURCE_BUCKET_NAME: props.sourceBucket.bucketName
      },
      role: this.rs.lambdaRole
    });
    const s3objectchecker = new sfntasks.LambdaInvoke(this, 's3objectchecker', {
      lambdaFunction: s3checker,
      resultPath: '$.RDSInfo'
    });
    
    //Lambda function that Redshift import CSV
    const kicker = new PythonFunction(this, 'LoadKicker', {
      runtime: lambda.Runtime.PYTHON_3_12,
      entry: 'lambda/redshiftloadkicker',
      handler: 'handler',
      memorySize: 256,
      timeout: cdk.Duration.minutes(15),
      environment: {
        REDSHIFT_WORKGROUP: this.rs.workgroupArn,
        REDSHIFT_NAMESPACE: this.rs.namespace,
        REDSHIFT_DATABASENAME: this.rs.database,
        DATASOURCE_BUCKET_NAME: props.sourceBucket.bucketName,
        ROLEARN_TO_READ_DATASOURCE: this.rs.rsRole.roleArn
      },
      role: this.rs.lambdaRole
    });
    // props.sourceBucket.grantRead(kicker);

    //Lambda function that check the status of Redshift processing
    const waiter = new PythonFunction(this, 'LoadWaiter', {
      runtime: lambda.Runtime.PYTHON_3_12,
      entry: 'lambda/redshiftloadwaiter',
      handler: 'handler',
      memorySize: 256,
      timeout: cdk.Duration.minutes(15),
      environment: {
        REDSHIFT_WORKGROUP: this.rs.workgroupArn,
        REDSHIFT_NAMESPACE: this.rs.namespace,
        REDSHIFT_DATABASENAME: this.rs.database,
      },
      role: this.rs.lambdaRole
    });

    //Lambda function that create table for BI
    const analysis = new PythonFunction(this, 'TableAnalysisForBI', {
      runtime: lambda.Runtime.PYTHON_3_12,
      entry: 'lambda/redshiftanalysistable',
      handler: 'handler',
      memorySize: 256,
      timeout: cdk.Duration.minutes(15),
      environment: {
        REDSHIFT_WORKGROUP: this.rs.workgroupArn,
        REDSHIFT_DATABASENAME: this.rs.database
      },
      role: this.rs.lambdaRole
    });

    //invoke lambda from Step Function
    const tableanalysis = new sfntasks.LambdaInvoke(this, 'SfnTableAnalysisForBI', {
      lambdaFunction: analysis,
    });


    //Define map state
    const itaratorRdsLoadKicker = new sfn.Map(this,'MapState',{
      // itemsPath:'$.bucket_list',
      itemsPath:sfn.JsonPath.stringAt('$.bucket_list'),
      resultPath:'$.Payload.result',
      // outputPath:'$.Payload',
      parameters: {
        'bucket.$': '$$.Map.Item.Value',
        'event.$': '$$',
      }
    })

    //Define dataloadSteps
    const startStep = new sfn.Pass(this, 'DataLoadSteps', {});
    const doneDataLoadSteps = this.dataloadSteps(startStep, kicker, waiter);

    rdsloadkicker.next(s3objectchecker)
    s3objectchecker.next(startStep)

    itaratorRdsLoadKicker.iterator(rdsloadkicker)
    itaratorRdsLoadKicker.next(tableanalysis)

    //Define StepFunction flow
    const sm = new sfn.StateMachine(this, 'StateMachine', {
      stateMachineName: 'makeTableWorkFlow',
      definition: setupkicker
        .next(wait)
        .next(itaratorRdsLoadKicker)
    });
    
    // Define EventBridge Scheduler
    const eventSchedulerRole = new iam.Role(this, "eventSchedulerRole", {
      assumedBy: new iam.ServicePrincipal("scheduler.amazonaws.com"),
    });
    eventSchedulerRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ["states:StartExecution"],
        resources: [sm.stateMachineArn]
      })
    )
    new scheduler.CfnSchedule(this, `execStepFunctionsSchedule`, {
      scheduleExpression: "cron(0 1 * * ? *)", // 毎日 01:00 に実行
      scheduleExpressionTimezone: "Asia/Tokyo", // タイムゾーンを JST で指定
      flexibleTimeWindow: { mode: "OFF" },
      target: {
        arn: sm.stateMachineArn,
        roleArn: eventSchedulerRole.roleArn,
        input: JSON.stringify({
        Mode: 'Operation', 
        Term: 'specific', 
        Day: 'nothing',
        waitSeconds: 600,
        bucket_list: ['products_master','orders_master','order_details_master']
      })
      },
      groupName: "default",
    });
  }

  //Redshift query and checking status of proccesing
  private dataloadSteps(before: sfn.INextable, kicker: lambda.IFunction, waiter: lambda.IFunction): sfn.INextable {
    const doneLoading = new sfn.Pass(this, `Done`);
    const kickerStep = new sfntasks.LambdaInvoke(this, `LoadKickerStep`, {
      lambdaFunction: kicker,
      resultPath: `$.loadKickerResult`,
      payload: sfn.TaskInput.fromObject({
        environments: sfn.JsonPath.objectAt('$'),
       })
    });
    const loop = new sfn.Wait(this, `Wait5SecondStep`, {
      time: sfn.WaitTime.duration(cdk.Duration.seconds(5))
    });
    kickerStep.next(loop);
    loop
      .next(
        new sfntasks.LambdaInvoke(this, `LoadWaiterStep`, {
          lambdaFunction: waiter,
          inputPath: `$.loadKickerResult.Payload`,
          resultPath: `$.loadWaiterResult`,
        })
      )
      .next(
        new sfn.Choice(this, `LambdaResultChoiceStep`)
          .when(sfn.Condition.booleanEquals(`$.loadWaiterResult.Payload`, false), loop)
          .otherwise(doneLoading)
      );

    before.next(kickerStep);
    return doneLoading;

  }
}
