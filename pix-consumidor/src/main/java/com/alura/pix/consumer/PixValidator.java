package com.alura.pix.consumer;

import com.alura.pix.avro.PixRecord;
import com.alura.pix.dto.PixDTO;
import com.alura.pix.dto.PixStatus;
import com.alura.pix.exception.KeyNotFoundException;
import com.alura.pix.model.Key;
import com.alura.pix.model.Pix;
import com.alura.pix.repository.KeyRepository;
import com.alura.pix.repository.PixRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

@Service
public class PixValidator {

    @Autowired
    private KeyRepository keyRepository;

    @Autowired
    private PixRepository pixRepository;

    @KafkaListener(topics = "pix-topic", groupId = "grupo")
    @RetryableTopic(
            //Tempo em milissegundos da primeira tentativa após o erro
            backoff = @Backoff(value = 3000L),
            //Tentativas de envio da mensagem, default 3
            attempts = "5",
            //Quanto a criação dos tópicos de erro ser automático
            //no caso criaria um tópico novo por tentativa e.g.: pix-topic1 ...
            autoCreateTopics = "true",
            //Quanto as exceções que deve haver as tentativas.
            include = KeyNotFoundException.class
    )
    public void processaPix(PixRecord pixRecord) {
        System.out.println("Pix  recebido: " + pixRecord.getIdentificador());

        Pix pix = pixRepository.findByIdentifier(pixRecord.getIdentificador().toString());

        Key origem = keyRepository.findByChave(pixRecord.getChaveOrigem().toString());
        Key destino = keyRepository.findByChave(pixRecord.getChaveDestino().toString());

        if (origem == null || destino == null) {
            pix.setStatus(PixStatus.ERRO);
            //throw new KeyNotFoundException("ERROR - Origem: "+ origem +", Destino: "+ destino );
        } else {
            pix.setStatus(PixStatus.PROCESSADO);
        }
        pixRepository.save(pix);
    }

}
