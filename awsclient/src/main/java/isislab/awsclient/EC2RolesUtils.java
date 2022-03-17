package isislab.awsclient;

public class EC2RolesUtils {
	
	protected static final String ROLE_EC2_POLICY_DOCUMENT =
		    "{" +
		    "  \"Version\": \"2012-10-17\"," +
		    "  \"Statement\": [" +
		    "    {" +
		    "        \"Sid\": \"VisualEditor0\"," +
		    "        \"Effect\": \"Allow\"," +
		    "        \"Action\": \"iam:CreateServiceLinkedRole\"," +
		    "        \"Resource\": \"*\"," +
		    "        \"Condition\": {" +
		    "            \"StringEquals\": {" +
		    "                \"iam:AWSServiceName\": [" +
		    "                    \"autoscaling.amazonaws.com\"," +
		    "                    \"ec2scheduled.amazonaws.com\"," +
		    "                    \"elasticloadbalancing.amazonaws.com\"," +
		    "                    \"spot.amazonaws.com\"," +
		    "                    \"spotfleet.amazonaws.com\"," +
		    "                    \"transitgateway.amazonaws.com\""+
		    "                ]"+
		    "           }"+
		    "         }"+
		    "    }," +
		    "	{" +
		    "        \"Sid\": \"VisualEditor1\"," +
		    "        \"Effect\": \"Allow\"," +
		    "        \"Action\": \"iam:CreateServiceLinkedRole\"," +
		    "        \"Resource\": \"arn:aws:iam::*:role/aws-service-role/ssm.amazonaws.com/AWSServiceRoleForAmazonSSM*\"," +
		    "        \"Condition\": {" +
		    "            \"StringLike\": {" +
		    "                \"iam:AWSServiceName\": \"ssm.amazonaws.com\"" +
		    "            }" +
		    "        }" +
		    "    }," +
		    "	 {" +
		    "        \"Sid\": \"VisualEditor2\"," +
		    "        \"Effect\": \"Allow\"," +
		    "        \"Action\": [" +
		    "            \"iam:GetServiceLinkedRoleDeletionStatus\"," +
		    "            \"iam:DeleteServiceLinkedRole\"" +
		    "        ]," +
		    "        \"Resource\": \"arn:aws:iam::*:role/aws-service-role/ssm.amazonaws.com/AWSServiceRoleForAmazonSSM*\"" +
		    "    }," +
		    "	{" +
		    "       \"Sid\": \"VisualEditor3\"," +
		    "       \"Effect\": \"Allow\"," +
		    "       \"Action\": [" +
		    "           \"organizations:ListRoots\"," +
		    "           \"cloudwatch:PutMetricData\"," +
		    "           \"ds:CreateComputer\"," +
		    "           \"logs:*\"," +
		    "           \"organizations:DescribeAccount\"," +
		    "           \"ssmmessages:OpenControlChannel\"," +
		    "           \"autoscaling:*\"," +
		    "           \"sqs:*\"," +
		    "           \"organizations:DescribePolicy\"," +
		    "           \"organizations:ListChildren\"," +
		    "           \"organizations:DescribeOrganization\"," +
		    "           \"ssmmessages:OpenDataChannel\"," +
		    "           \"organizations:DescribeOrganizationalUnit\"," +
		    "           \"ec2messages:*\"," +
		    "           \"ec2:DescribeInstanceStatus\"," +
		    "           \"organizations:ListPoliciesForTarget\"," +
		    "           \"organizations:ListTargetsForPolicy\"," +
		    "           \"s3:*\"," +
		    "           \"elasticloadbalancing:*\"," +
		    "           \"ssmmessages:CreateControlChannel\"," +
		    "           \"s3-object-lambda:*\"," +
		    "           \"organizations:ListPolicies\"," +
		    "           \"ssmmessages:CreateDataChannel\"," +
		    "           \"iam:*\"," +
		    "           \"cloudwatch:*\"," +
		    "           \"ssm:*\"," +
		    "           \"ec2:*\"," +
		    "           \"organizations:ListParents\"," +
		    "           \"ds:DescribeDirectories\"" +
		    "       ]," +
		    "       \"Resource\": \"*\"" +
		    "   }" +
	    "   ]" +
	    "}";
	
	protected static final String ASSUME_ROLE_DOCUMENT =
		    "{" +
		    "  \"Version\": \"2012-10-17\"," +
		    "  \"Statement\": {" +
		    "		\"Effect\": \"Allow\"," +
		    "    	\"Principal\": {\"Service\": \"ec2.amazonaws.com\"}," +
		    "    	\"Action\": \"sts:AssumeRole\""+
		    "  }" +
		    "}";
}
