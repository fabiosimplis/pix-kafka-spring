package com.alura.pix.streams;

import com.alura.pix.dto.PixDTO;
import com.alura.pix.serdes.PixSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class PixAggregator {

    @Autowired
    public void aggregator(StreamsBuilder streamsBuilder){

        KStream<String, PixDTO> messageStream = streamsBuilder
                .stream("pix-topic", Consumed.with(Serdes.String(), PixSerdes.serdes()))
                .peek((key, value) -> System.out.println("Pix recebido de : " + value.getChaveOrigem() + ", Valor = " + value.getValor()))
                .filter((key, value) -> value.getValor() < 1000);

        messageStream.print(Printed.toSysOut());
        // Envia para outro tópico
        messageStream.to("pix-topic-verificacao-fraude", Produced.with(Serdes.String(), PixSerdes.serdes()));
    }
}
