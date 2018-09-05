package ink.baixin.ripple.scheduler.services

import awscala.CredentialsLoader
import awscala.ec2.EC2
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingClientBuilder
import com.amazonaws.services.elasticloadbalancingv2.model.{DescribeTargetGroupsRequest, DescribeTargetHealthRequest}
import ink.baixin.ripple.scheduler.AWSConfig

import scala.collection.JavaConverters._

object AWSService {
  lazy val credentialsProvider = CredentialsLoader.load()
  lazy val elbClient = AmazonElasticLoadBalancingClientBuilder
    .standard()
    .withCredentials(credentialsProvider)
    .withRegion(AWSConfig.region.getName)
    .build()
  lazy val ec2Client = EC2.at(AWSConfig.region)

  def getELBTargetsURIs(tg_arn: String): Seq[String] = {
    val tgRequest = new DescribeTargetGroupsRequest().withTargetGroupArns(tg_arn)
    val tgResult = elbClient.describeTargetGroups(tgRequest).getTargetGroups

    if (tgResult.isEmpty) return Seq[String]()

    val tg = tgResult.get(0)

    val request = new DescribeTargetHealthRequest().withTargetGroupArn(tg_arn)
    val result = elbClient.describeTargetHealth(request).getTargetHealthDescriptions

    val targets = result.asScala.map(_.getTarget)
    val domains = ec2Client.instances(targets.map(_.getId)).map {
      instance => instance.instanceId -> instance.privateDnsName
    }.toMap

    targets.map { target =>
      s"${tg.getProtocol.toLowerCase}://${domains(target.getId)}:${target.getPort}"
    }
  }
}