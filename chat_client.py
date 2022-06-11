#!/usr/bin/env python3

import sys
import threading
import re

from kafka import KafkaProducer, KafkaConsumer


should_quit = False
LIST_CHAN_SUB = []
MODERATION_TOPIC = "chat_moderation"

def read_messages(consumer):
    # TODO À compléter
    while not should_quit:
        # On utilise poll pour ne pas bloquer indéfiniment quand should_quit
        # devient True
        received = consumer.poll(10000)

        for channel, messages in received.items():
            for msg in messages:
                str_msg = '<' + str(msg.key, encoding='utf-8') + '> ' + str(msg.value, encoding ='utf-8')
                print("< %s: %s" % (channel.topic, str_msg))
        



def cmd_msg(username, producer, channel, line):
    if(channel == None):
        print("ERROR: Can't send your message, you are not in a channel")
    else:
        producer.send(chan_to_topic(channel), bytes(line, 'utf-8'), bytes(username, 'utf-8'))
        producer.send(MODERATION_TOPIC, bytes(line, 'utf-8'), bytes(username, 'utf-8'))


def cmd_join(consumer, producer, chan_to_join, username):
    channel_ok = re.match(r'^#[a-zA-Z0-9_-]+$',chan_to_join)
    if not channel_ok:
        print("ERROR: " + chan_to_join + " as a channel name is not handled")
        return False
    else:
        LIST_CHAN_SUB.append(chan_to_join)
        new_list_topic = []
        for chan in LIST_CHAN_SUB:
            new_list_topic.append(chan_to_topic(chan))
        consumer.subscribe(new_list_topic)
        producer.send(chan_to_topic(chan_to_join), bytes(username + ' has joined the channel', 'utf-8'))
        print(consumer.subscription())
        return True

def cmd_part(consumer, producer, chan_quit, username, chan_active):
    channel_unsub_ok = re.match(r'^#[a-zA-Z0-9_-]+$',chan_quit)
    if not channel_unsub_ok:
        print("ERROR: " + chan_quit + " as a channel name is not handled")
        return chan_active
    if chan_quit in LIST_CHAN_SUB:
        LIST_CHAN_SUB.remove(chan_quit)
        if len(LIST_CHAN_SUB) < 1:
            consumer.unsubscribe()
            print("You are not subscribe a channel anymore")
            producer.send(chan_to_topic(chan_quit), bytes(username + ' has left the channel', 'utf-8'))
            return None
        else:
            update_list_topic = []
            for chan in LIST_CHAN_SUB:
                update_list_topic.append(chan_to_topic(chan))
            consumer.subscribe(update_list_topic)
            print(consumer.subscription())
            producer.send(chan_to_topic(chan_quit), bytes(username + ' has left the channel', 'utf-8'))
            return LIST_CHAN_SUB[0]
    else:
        print("ERROR: " + chan_quit + " is not part of your subscribed channel")
        return chan_active

def cmd_active(chan_to_active, active_chan_current):
    channel_unsub_ok = re.match(r'^#[a-zA-Z0-9_-]+$',chan_to_active)
    if not channel_unsub_ok:
        print("ERROR: " + chan_to_active + " as a channel name is not handled")
        return active_chan_current
    if not chan_to_active in LIST_CHAN_SUB:
        print("You are not yet subscribe to this channel, join (/join) the channel to make it your active channel")
        return active_chan_current
    if chan_to_active == active_chan_current:
        print(chan_to_active + ": this channel is already your active channel")
        return active_chan_current
    else:
        return chan_to_active

# cette méthode permet de transformer le nom d'un channel standard (#[nomchannel]) en nom de topic standard (de type 'chat_channel_[nomchannel]')
def chan_to_topic(chan):
    return "chat_channel_" + chan[1:]

def cmd_quit(consumer, producer, username):
    for chan in LIST_CHAN_SUB:
        producer.send(chan_to_topic(chan), bytes(username + ' has quit the chat', 'utf-8'))
    consumer.unsubscribe()



def main_loop(username, consumer, producer):
    curchan = None
    LIST_CHAN_SUB=[]
    
    while True:
        try:
            if curchan is None:
                line = input("> ")
            else:
                line = input("[#%s]> " % curchan)
        except EOFError:
            print("/quit")
            line = "/quit"

        if line.startswith("/"):
            cmd, *args = line[1:].split(" ", maxsplit=1)
            cmd = cmd.lower()
            args = None if args == [] else args[0]
        else:
            cmd = "msg"
            args = line

        if cmd == "msg":
            cmd_msg(username, producer, curchan, args)
        elif cmd == "join":
            if cmd_join(consumer, producer, args, username):
                curchan = args
        elif cmd == "part":
            curchan = cmd_part(consumer, producer, args, username, curchan)
        elif cmd == "active":
            curchan = cmd_active(args,curchan)
        elif cmd == "quit":
            cmd_quit(consumer, producer, username)
            LIST_CHAN_SUB = []
            break

def main():
    if len(sys.argv) != 2:
        print("usage: %s username" % sys.argv[0])
        return 1

    username = sys.argv[1]
    consumer = KafkaConsumer() ## par defaut il se met sur le localhost 9092
    producer = KafkaProducer()
    th = threading.Thread(target=read_messages, args=(consumer,))
    th.start()

    try:
        main_loop(username, consumer, producer)
    finally:
        global should_quit
        should_quit = True
        th.join()



if __name__ == "__main__":
    sys.exit(main())
