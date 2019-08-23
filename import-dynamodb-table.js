const AWS = require('aws-sdk');
const db = new AWS.DynamoDB.DocumentClient();
const s3 = new AWS.S3();

exports.handler = (event, context, callback) => {
	const { BUCKET_NAME, FILE_NAME, TABLE_NAME } = process.env;
	s3.getObject(
		{
			Bucket: BUCKET_NAME,
			Key: FILE_NAME,
		},
		(err, { Body }) => {
			if (err) {
				console.log(err);
				return;
			}

			JSON.parse(Body).forEach(async (item) => {
				try {
					await new Promise((resolve, reject) => {
						db.put(
							{
								TableName: TABLE_NAME,
								Item: item
							},
							err => err ? reject(err) : resolve()
						);
					});
				}
				catch(e) {
					console.log(e);
				}
			});

			console.log(`Data was successfully imported into ${TABLE_NAME}!`)
		}
	);
};