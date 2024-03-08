package ai.yles.service;

import ai.yles.service.common.ElasticClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.ExistsAliasRequest;
import co.elastic.clients.elasticsearch.indices.GetAliasResponse;
import co.elastic.clients.elasticsearch.indices.UpdateAliasesRequest;
import co.elastic.clients.elasticsearch.indices.UpdateAliasesResponse;
import co.elastic.clients.elasticsearch.indices.get_alias.IndexAliases;
import co.elastic.clients.elasticsearch.indices.update_aliases.Action;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.*;

public class ElasticSearchMetaInfo {
    private static ElasticsearchClient client = null;
    private static ElasticSearchMetaInfo metaInfo = null;

    public static synchronized ElasticSearchMetaInfo build() {
        if (Objects.isNull(metaInfo)) {
            metaInfo = new ElasticSearchMetaInfo();
        }
        return metaInfo;
    }

    private ElasticSearchMetaInfo() {
        client = ElasticClient.build();
    }

    public String getAliasRealIndex(String alias) throws IOException {
        if (checkAliasExists(alias)) {
            GetAliasResponse aliasResponse = client.indices().getAlias(builder -> builder.name(alias));
            Map<String, IndexAliases> result = aliasResponse.result();
            System.out.println("alias result:" + result);
            Optional<Map.Entry<String, IndexAliases>> first = result.entrySet().stream().findFirst();
            return first.get().getKey();
        }
        return null;
    }

    public Boolean checkAliasExists(String alias) throws IOException {
        ExistsAliasRequest request = new ExistsAliasRequest.Builder().name(alias).build();
        BooleanResponse booleanResponse = client.indices().existsAlias(request);
        return booleanResponse.value();
    }

    public void switchAliase(String alias, String oldIndex, String newIndex) throws IOException {
        if (StringUtils.isNotEmpty(oldIndex)) {
            List<Action> actions = new ArrayList<Action>();
            actions.add(new Action.Builder().remove(builder -> builder.index(oldIndex).alias(alias)).build());
            actions.add(new Action.Builder().add(builder -> builder.index(newIndex).alias(alias)).build());
            UpdateAliasesRequest request = new UpdateAliasesRequest.Builder().actions(actions).build();
            System.out.println("switchAliase: " + request.toString());
            UpdateAliasesResponse updateAliasesResponse = client.indices().updateAliases(request);
            if (!updateAliasesResponse.acknowledged()) {
                System.out.println("updateAliasesResponse.acknowledged() is not true");
            }
        }
    }
}
