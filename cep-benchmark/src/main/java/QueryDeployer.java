import es.upm.lsd.cep.client.CEPDriverManager;
import es.upm.lsd.cep.client.OrchestratorConnection;
import es.upm.lsd.cep.client.factory.AutomaticQueryFactory;
import es.upm.lsd.cep.client.factory.CustomOperatorConfig;
import es.upm.lsd.cep.client.factory.FilterOperatorConfig;
import es.upm.lsd.cep.common.exception.CepException;
import es.upm.lsd.cep.common.exception.CepQueryFactoryException;
import es.upm.lsd.cep.common.exception.CepSubQueryFactoryException;
import es.upm.lsd.cep.common.json.driver.DeployQueryJSON;
import es.upm.lsd.cep.common.json.driver.RegisterQueryJSON;
import es.upm.lsd.cep.common.json.driver.ResponseStatusJSON;
import es.upm.lsd.cep.common.type.EndPointDescriptor;
import es.upm.lsd.cep.common.type.Schema;
import org.apache.commons.cli.*;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.logging.Logger;

public class QueryDeployer
{
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(QueryDeployer.class);

    private static String CLIENT_ID = "YSBQueryDeployer";

    private static String QUERY_NAME = "YSB-Advertising";

    private static int QUERY_INSTANCES = 1;

    private static OrchestratorConnection connection = null;

    // Input Stream
    private static String INPUT_STREAM = "MyInputStream";
    private static String INPUT_STREAM_FILEPATH = QueryDeployer.class.getResource("data.csv").getFile();

    // Output Stream
    private static String OUTPUT_STREAM = "MyOutputStream";
    private static String OUTPUT_STREAM_FILEPATH = INPUT_STREAM_FILEPATH + "filtered";

    private String hostname;
    private int port;

    private void parseCommandLineArgs(String[] args) {
        Options options = new Options();

        options.addOption(Option.builder()
                .required()
                .hasArg()
                .longOpt("--hostname")
                .build());

        options.addOption(Option.builder()
                .required()
                .hasArg()
                .longOpt("--port")
                .build());

        CommandLineParser parser = new DefaultParser();

        try {
            CommandLine cmd = parser.parse(options, args);

            hostname = cmd.getOptionValue("--hostname");
            port = Integer.valueOf(cmd.getOptionValue("--port"));

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public  static void main(String args[]) {

        try {
            connection = CEPDriverManager.getConnection(CLIENT_ID);
            connection.connect();
            LOG.info("Successfully connected to CEP Orchestrator");

            // Remove query if it exists
            connection.unRegisterQuery(QUERY_NAME);

            // Create a new query
            AutomaticQueryFactory queryFactory = new AutomaticQueryFactory(QUERY_NAME, QUERY_INSTANCES);
            RegisterQueryJSON queryJSON = null;
            DeployQueryJSON deployQueryJSON = null;

            try {
                // Define input source
                Schema schema = new Schema(10);
                schema.addIntegerField("survived");
                schema.addTextField("sex");
                schema.addIntegerField("age");
                schema.addIntegerField("n_siblings_spouses");
                schema.addTextField("parch");
                schema.addTextField("fare");
                schema.addTextField("class");
                schema.addTextField("deck");
                schema.addTextField("embark_town");
                schema.addTextField("alone");

                queryFactory.addStream(INPUT_STREAM, schema, INPUT_STREAM_FILEPATH);

                queryFactory.addStream(OUTPUT_STREAM, schema);

                // Define operator
                FilterOperatorConfig filterOperatorConfig = new FilterOperatorConfig("FilterOp", INPUT_STREAM,
                        OUTPUT_STREAM);
                filterOperatorConfig.setPredicate("sex == 'male'");

                queryFactory.addOperator(filterOperatorConfig);

//                CustomOperatorConfig customOperatorConfig = new CustomOperatorConfig("", )

                // Define output sink
                queryFactory.registerDataSink(OUTPUT_STREAM, OUTPUT_STREAM_FILEPATH);


                queryJSON = queryFactory.getQueryDescriptor();
                deployQueryJSON = queryFactory.getQueryDeploymentDescriptor();

            } catch (CepQueryFactoryException e) {
                LOG.error(e.getMessage());
            } catch (CepSubQueryFactoryException e) {
                LOG.error(e.getMessage());
            }

            if (queryJSON != null) {
                ResponseStatusJSON responseStatusJSON = connection.registerQuery(queryJSON);

                String msg = responseStatusJSON.getQueryName() + ' ' + responseStatusJSON.getResponseDescription() + ' '
                        + responseStatusJSON.getImId() + ' ' + responseStatusJSON.getResponseCode();
                LOG.info(msg);

                responseStatusJSON = connection.deployQuery(deployQueryJSON);

                msg = responseStatusJSON.getQueryName() + ' ' + responseStatusJSON.getResponseDescription() + ' '
                        + responseStatusJSON.getImId() + ' ' + responseStatusJSON.getResponseCode();
                LOG.info(msg);

            }
        } catch (CepException e) {
            LOG.error(e.getMessage());
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }
}
