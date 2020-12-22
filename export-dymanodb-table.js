const AWS = require('aws-sdk');
const db = new AWS.DynamoDB();
const docClient = new AWS.DynamoDB.DocumentClient();
const s3 = new AWS.S3();

exports.handler = (event, context, callback) => {
    const {
        BUCKET_NAME,
        KEY_NAME,
        TABLE_NAME,
        EXTENSION,
    } = process.env;

    db.describeTable({ TableName: TABLE_NAME }, (err, { Table }) => {
        if (err) {
            console.log(err);
        }

        const tableSizeMB = Math.ceil(
            Table.TableSizeBytes * ((1 / 1000) ** 2)
        );

        // DynamoDB scanned items maximum dataset size limit is 1 MB.
        // We need to do parallel scan if we exceed that max.
        for (let i = 0; i < tableSizeMB; i++) {
            docClient.scan(
                {
                    TableName: TABLE_NAME,
                    Select: 'ALL_ATTRIBUTES',
                    TotalSegments: tableSizeMB,
                    Segment: i,
                },
                (err, data) => {
                    if (err) {
                        console.log(err);
                    }

                    let keyName = i > 0 ? `${KEY_NAME}-${i}` : KEY_NAME;
                    s3.putObject(
                        {
                            Bucket: BUCKET_NAME,
                            Key: `${keyName}${EXTENSION}`,
                            Body: JSON.stringify(data.Items),
                        },
                        err => {
                            if (err) {
                                console.log(err);
                            }

                            console.log(`Successfully exported to ${BUCKET_NAME}/${keyName}`);
                        }
                    );
                }
            );
        }
    });
};