import * as cdk from 'aws-cdk-lib';
import { Stack, ScopedAws } from 'aws-cdk-lib';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { Construct } from 'constructs';
import { WorkFlow } from './stepfunctions';
import {config} from '../config/dev';

interface AnalysisSystemStackProps extends cdk.StackProps{
  vpc: ec2.IVpc;
  dbName: string;
  dbPort: string;
  dbHostname: string;
  dbSecret: cdk.aws_secretsmanager.ISecret;
}


export class AnalysisSystemStack extends Stack {
  constructor(scope: Construct, id: string, props: AnalysisSystemStackProps) {
    super(scope, id, props);

    //Define S3 bucket
    const dataBucket = new s3.Bucket(this, 'CsvBucket', {
      bucketName: `${config.account}-dwh-${config.region}`,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true
    });

    new WorkFlow(this, 'WorkFlow', {
      sourceBucket: dataBucket,
      vpc: props.vpc,
      dbName: props.dbName,
      dbPort: props.dbPort,
      dbHostname: props.dbHostname,
      dbSecret: props.dbSecret
    });
  }
}
