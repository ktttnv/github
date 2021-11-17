package io.cdap.plugin.github.source;

import com.google.gson.Gson;
import io.cdap.plugin.github.source.batch.GithubBatchSourceConfig;
import io.cdap.plugin.github.source.batch.GithubFormatProvider;
import io.cdap.plugin.github.source.batch.GithubInputFormat;
import io.cdap.plugin.github.source.common.model.impl.Commit;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.hadoop.WritableCoder;
import org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;

public class BeamAdapter {


    public static void main(String[] args) {
        Gson gson = new Gson();
        Configuration myHadoopConfiguration = new Configuration(false);

        String authorizationToken = System.getenv("GITHUB_PAT");
        String repoOwner = "ktttnv";
        String repoName = "react-quiz";
        String datasetName = "Commits";
        String hostname = "https://api.github.com";

        GithubBatchSourceConfig githubBatchSourceConfig = gson.fromJson(
                String.format("{\"authorizationToken\":\"%s\",\"repoOwner\":\"%s\",\"repoName\":\"%s\",\"datasetName\":\"%s\",\"hostname\":\"%s\"}",
                        authorizationToken, repoOwner, repoName, datasetName, hostname),
                GithubBatchSourceConfig.class);
        GithubFormatProvider githubFormatProvider = new GithubFormatProvider(githubBatchSourceConfig);
        myHadoopConfiguration.setClass("mapreduce.job.inputformat.class", GithubInputFormat.class,
                InputFormat.class);
        myHadoopConfiguration.setClass("key.class", Text.class, Object.class);
        myHadoopConfiguration.setClass("value.class", Commit.class, Object.class);
        myHadoopConfiguration.set(GithubFormatProvider.PROPERTY_CONFIG_JSON,
                githubFormatProvider.getInputFormatConfiguration()
                        .get(GithubFormatProvider.PROPERTY_CONFIG_JSON));

        Pipeline p = Pipeline.create();

        // Read data only with Hadoop configuration.
        PCollection<KV<Text, Commit>> pcol = p.apply("read",
                HadoopFormatIO.<Text, Commit>read()
                        .withConfiguration(myHadoopConfiguration)
        ).setCoder(
                KvCoder.of(NullableCoder.of(WritableCoder.of(Text.class)), SerializableCoder.of(Commit.class)));

        PCollection<String> strings = pcol.apply(MapElements
                        .into(TypeDescriptors.strings())
                        .via(
                                ((SerializableFunction<KV<Text, Commit>, String>) input -> {
                                    Gson gson1 = new Gson();
                                    return gson1.toJson(input.getValue());
                                })
                        )
                )
                .setCoder(StringUtf8Coder.of());

        strings.apply(TextIO.write().to("./txt.txt"));
        p.run();
    }
}