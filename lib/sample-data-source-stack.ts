import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { PythonFunction } from '@aws-cdk/aws-lambda-python-alpha';

export class SampleDataSourceStack extends cdk.Stack {
  readonly vpc: ec2.Vpc;
  readonly dbName: string;
  readonly dbPort: string;
  readonly dbHostname: string;
  readonly dbSecret: cdk.aws_secretsmanager.ISecret;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    this.vpc = new ec2.Vpc(this, 'Vpc', {
      subnetConfiguration: [
        {
        name: "private",
        subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
        },
        {
        name: "public",
        subnetType: ec2.SubnetType.PUBLIC,
        mapPublicIpOnLaunch: false
        },
      ]
      
    });
    
    const db = new Database(this, 'Database', {
      vpc: this.vpc
    });
    this.dbName = db.dbName;
    this.dbPort = db.dbport; 
    this.dbHostname = db.dbHostname;
    this.dbSecret = db.dbSecret;
  }
}

interface DatabaseProps {
  vpc: ec2.IVpc;
}

class Database extends Construct {
  readonly dbName: string;
  readonly dbport: string;
  readonly dbHostname: string;
  readonly dbSecret: cdk.aws_secretsmanager.ISecret;
  
  constructor(scope: Construct, id: string, props: DatabaseProps) {
    super(scope, id);

    this.dbName = 'sample';
    this.dbport = '3306';
    
    // Aurora Security Group
    const rdssg = new ec2.SecurityGroup(this, 'RDSSG', {
      vpc: props.vpc
    });
    rdssg.addIngressRule(ec2.Peer.ipv4(props.vpc.vpcCidrBlock), ec2.Port.tcp(Number(this.dbport)));

    //Aurora MySQL
    const dbCluster = new rds.DatabaseCluster(this, 'AuroraCluster', {
      engine: rds.DatabaseClusterEngine.auroraMysql({ version: rds.AuroraMysqlEngineVersion.VER_3_04_0 }),
      defaultDatabaseName: this.dbName,
      instances: 1,
      instanceProps: {
        instanceType: ec2.InstanceType.of(ec2.InstanceClass.R6G, ec2.InstanceSize.LARGE),
        vpc: props.vpc,
        vpcSubnets: {
          subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS
        },
        securityGroups: [rdssg]
      },
      storageEncrypted: true,
      cloudwatchLogsExports: ['general']
    });

    // Aurora Credentials
    const secret = dbCluster.secret!;

    const seeder = new PythonFunction(this, 'Seeder', {
      runtime: lambda.Runtime.PYTHON_3_12,
      entry: 'lambda/seeding',
      handler: 'handler',
      vpc: props.vpc,
      memorySize: 128,
      timeout: cdk.Duration.minutes(15), 
      environment: {
        DB_HOSTNAME: dbCluster.clusterEndpoint.hostname,
        DB_PORT: this.dbport,
        DB_NAME: this.dbName,
        DB_SECRET_NAME: secret.secretName
      }
    });
    secret.grantRead(seeder); 
    this.dbHostname = dbCluster.clusterEndpoint.hostname;
    this.dbSecret = dbCluster.secret!;
  }
}
