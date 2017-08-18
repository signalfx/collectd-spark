html_text = """
<!DOCTYPE html><html>
      <head>
        <meta http-equiv="Content-type" content="text/html;
        charset=utf-8"/><link rel="stylesheet"
        href="/static/bootstrap.min.css"
        type="text/css"/><link rel="stylesheet"
        href="/static/vis.min.css" type="text/css"/>
        <link rel="stylesheet" href="/static/webui.css"
        type="text/css"/>
        <link rel="stylesheet" href="/static/timeline-view.css"
        type="text/css"/>
        <script src="/static/sorttable.js"></script>
        <script src="/static/jquery-1.11.1.min.js"></script>
        <script src="/static/vis.min.js"></script>
        <script src="/static/bootstrap-tooltip.js"></script>
        <script src="/static/initialize-tooltips.js"></script>
        <script src="/static/table.js"></script>
        <script src="/static/additional-metrics.js"></script>
        <script src="/static/timeline-view.js"></script>
        <script src="/static/log-view.js"></script>
        <script src="/static/webui.js"></script><script>setUIRoot('')</script>

        <title>Application: JavaNetworkWordCount</title>
      </head>
      <body>
        <div class="container-fluid">
          <div class="row-fluid">
            <div class="span12">
              <h3 style="vertical-align: middle; display: inline-block;">
                <a style="text-decoration: none" href="/">
                  <img src="/static/spark-logo-77x50px-hd.png"/>
                  <span class="version" style="margin-right: 15px;">2.2.0
                  </span>
                </a>
                Application: JavaNetworkWordCount
              </h3>
            </div>
          </div>
          <div class="row-fluid">
        <div class="span12">
          <ul class="unstyled">
            <li><strong>ID:</strong> app-20170816201805-0010</li>
            <li><strong>Name:</strong> JavaNetworkWordCount</li>
            <li><strong>User:</strong> root</li>
            <li><strong>Cores:</strong>
            Unlimited (2 granted)
            </li>
            <li>
              <span data-toggle="tooltip" title="Maximum number of executors
              that this application will use. This limit is finite only when
              dynamic allocation is enabled. The number of granted executors
              may exceed the limit
              ephemerally when executors are being killed.
    " data-placement="right">
                <strong>Executor Limit: </strong>
                Unlimited
                (2 granted)
              </span>
            </li>
            <li>
              <strong>Executor Memory:</strong>
              512.0 MB
            </li>
            <li><strong>Submit Date:</strong> 2017/08/16 20:18:05</li>
            <li><strong>State:</strong> RUNNING</li>
            <li><strong>
                <a href="http://172.31.15.190:4040">Application Detail UI</a>
                </strong></li>
          </ul>
        </div>
      </div><div class="row-fluid"> <!-- Executors -->
        <div class="span12">
          <h4> Executor Summary </h4>
          <table class="table table-bordered table-condensed
          table-striped sortable">
      <thead><th width="" class="">ExecutorID</th>
      <th width="" class="">Worker</th><th width="" class="">Cores</th>
      <th width="" class="">Memory</th><th width="" class="">State</th>
      <th width="" class="">Logs</th></thead>
      <tbody>
        <tr>
      <td>1</td>
      <td>
        <a href="http://172.31.8.55:8081">
        worker-20170810211404-172.31.8.55-33130</a>
      </td>
      <td>1</td>
      <td>512</td>
      <td>RUNNING</td>
      <td>
        <a href=
        "http://172.31.8.55:8081/logPage?
        appId=app-20170816201805-0010&amp;executorId=1&amp;logType=stdout">
        stdout</a>
        <a href=
        "http://172.31.8.55:8081/logPage?
        appId=app-20170816201805-0010&amp;executorId=1&amp;logType=stderr">
        stderr</a>
      </td>
    </tr><tr>
      <td>0</td>
      <td>
        <a href="http://172.31.15.190:8081">
        worker-20170810211415-172.31.15.190-33702</a>
      </td>
      <td>1</td>
      <td>512</td>
      <td>RUNNING</td>
      <td>
        <a href=
        "http://172.31.15.190:8081/logPage?
        appId=app-20170816201805-0010&amp;executorId=0&amp;logType=stdout">
        stdout</a>
        <a href=
        "http://172.31.15.190:8081/logPage?
        appId=app-20170816201805-0010&amp;executorId=0&amp;logType=stderr">
        stderr</a>
      </td>
    </tr>
      </tbody>
    </table>
        </div>
      </div>
        </div>
      </body>
    </html>
"""


class MockHTMLResponse(object):

    def read(self):
        return html_text
