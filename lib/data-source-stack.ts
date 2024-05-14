import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as sm from 'aws-cdk-lib/aws-secretsmanager';
import * as iam from 'aws-cdk-lib/aws-iam';

export interface dataSourceStackProps extends cdk.StackProps {

  readonly rdsIds?: {
    readonly dbClusterId: string;
    readonly secretId: string;
  };
  readonly vpcId: string;
  readonly dbPort: string;
  readonly dbName: string;
  readonly dbHostname: string;

}

export class dataSourceStack extends cdk.Stack {
  readonly vpc: ec2.IVpc;
  readonly dbName: string;
  readonly dbPort: string;
  readonly dbHostname: string;
  readonly dbSecret: cdk.aws_secretsmanager.ISecret;

  constructor(scope: Construct, id: string, props?: dataSourceStackProps) {
    super(scope, id, props);
    const dbCluster = rds.DatabaseCluster.fromDatabaseClusterAttributes(this, "SampleDataSource", {
          clusterIdentifier: props!.rdsIds!.dbClusterId,
        })
    const secret = sm.Secret.fromSecretAttributes(this, "Secret", {
            secretPartialArn: cdk.Arn.format(
              {
                service: "secretsmanager",
                resource: "secret",
                arnFormat: cdk.ArnFormat.COLON_RESOURCE_NAME,
                resourceName: props!.rdsIds!.secretId,
              },
              this
            )
          });

    if (props?.rdsIds?.dbClusterId){
    this.vpc = ec2.Vpc.fromLookup(this, 'Vpc', {
      vpcId: props?.vpcId
    })
    this.dbName = props.dbName;
    this.dbPort = props.dbPort;
    this.dbHostname = props.dbHostname;
    this.dbSecret = secret;
  }
  }
}