1.1修复了log_record_view_0 events拆分合并的问题,从数组变为追加

1.2增加对event转义处理

1.3修复有几率出现的Spooling Directory中文件被修改导致等问题导致agent终止
   (1)decode失败增加IGNORE选项的支持
   (2)文件修改时间检验,如果一段时间内修改则放弃读取该文件