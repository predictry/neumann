#!/bin/bash

export APP_VERSION=`git rev-parse --short HEAD`
export APP_NAME=neumann
export ENV_NAME=${APP_NAME}-env
export S3_BUCKET=elasticbeanstalk-deployment-bundles
export S3_CONFIG_BUCKET=predictry/configuration

#AWS credentials

export AWS_ACCESS_KEY_ID=AKIAIFCXFZVU6PA7JGHA
export AWS_SECRET_ACCESS_KEY=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
export AWS_DEFAULT_REGION=ap-southeast-1

if [ -z "$WORKSPACE" ]; then
   WORKSPACE=.
fi

# clean build artifacts and create the application archive (also ignore any files named .git* in any folder)
#git clean -fd

# precompile assets, ...

cd "$WORKSPACE/"

#download config files
echo "Downloading config files from S3"
aws s3 sync s3://${S3_CONFIG_BUCKET}/${APP_NAME}/ .

# zip the application
echo "Zipping source and config files"
zip -x "*.git*" "*app-env/*"  "*.db" "${APP_NAME}-${APP_VERSION}.zip*" ".idea*" @ -r "${APP_NAME}-${APP_VERSION}.zip" .

# delete any version with the same name (based on the short revision)
echo "Deleting existing app-version"
aws elasticbeanstalk delete-application-version --application-name "${APP_NAME}" --version-label "${APP_VERSION}"  --delete-source-bundle

# upload to S3
echo "Uploading zip archive to AWS-EB"
aws s3 cp ${APP_NAME}-${APP_VERSION}.zip s3://${S3_BUCKET}/${APP_NAME}-${APP_VERSION}.zip

# create a new version and update the environment to use this version
#aws elasticbeanstalk create-application-version --application-name "${APP_NAME}" --version-label "${APP_VERSION}" --source-bundle S3Bucket="${S3_BUCKET}",S3Key="${APP_NAME}-${APP_VERSION}.zip"
#aws elasticbeanstalk update-environment --environment-name "${ENV_NAME}" --version-label "${APP_VERSION}"

echo "Create app-version"
aws elasticbeanstalk create-application-version --application-name "${APP_NAME}" --version-label "${APP_VERSION}" --source-bundle  S3Bucket="${S3_BUCKET}",S3Key="${APP_NAME}-${APP_VERSION}.zip"
echo "Updating app environment"
aws elasticbeanstalk update-environment --environment-name "${ENV_NAME}" --version-label "${APP_VERSION}"

echo "Deleting zip archive"
rm "${APP_NAME}-${APP_VERSION}.zip"
