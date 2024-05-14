type Config = {
    account: string,
    region: string
    isExistDB:boolean,
    dbClusterId:string,
    dbPort: string,
    dbName: string,
    dbHostname: string,
    secretId:string,
    vpcId:string,
}
export const config:Config = {
    account: "", //AWS アカウントの ID を入力してください
    region: "", //RDS が配置されている DB のリージョンを入力してください
    isExistDB:false, //既存 DB を使う場合は false にして、以下の config の値を入力してください。
    dbClusterId: "", //Aurora の場合は クラスター ID を、RDS の場合はインスタンス ID を入力してください
    dbPort: "", //DB のポート番号を入力してください
    dbName: "", //DB 名 (defaultdatabasename) を入力してください
    dbHostname:"", //"DB のエンドポイント名を入力してください
    secretId: "", //Secrets Manager から、該当の RDS を選択してシークレット名を入力してください
    vpcId: "", //RDS が配置されている VPC のID を入力してください
}