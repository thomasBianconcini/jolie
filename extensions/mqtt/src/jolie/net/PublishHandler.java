/*
 * Copyright (C) 2017 stefanopiozingaro
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package jolie.net;

import io.netty.buffer.ByteBuf;

/**
 * Interface letting the user to implement hot to react at the arrival of a
 * specific topic.
 *
 * @author stefanopiozingaro
 */
public interface PublishHandler {

    /**
     * Abstract method setting the behaviour of subscriber.
     *
     * @param topic {@link String}
     * @param payload Netty {@link ByteBuf}
     */
    void handleMessage(String topic, ByteBuf payload);

}
