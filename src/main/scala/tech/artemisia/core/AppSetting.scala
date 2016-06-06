package tech.artemisia.core

/**
 * Created by chlr on 5/23/16.
 */


case class AppSetting(cmd: Option[String] = Some("run"), value: Option[String] = None, context: Option[String] = None
                      , config: Option[String] = None, run_id: Option[String] = None, working_dir: Option[String] = None,
                      skip_checkpoints: Boolean = false, component: Option[String] = None, task: Option[String] = None )
