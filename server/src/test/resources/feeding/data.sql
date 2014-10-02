SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Records of acl_class
-- ----------------------------
INSERT INTO `acl_class` VALUES ('1', 'com.kylinolap.cube.CubeInstance');
INSERT INTO `acl_class` VALUES ('2', 'com.kylinolap.index.job.JobInstance');

-- ----------------------------
-- Records of authorities
-- ----------------------------
INSERT INTO `authorities` VALUES ('ADMIN', 'ROLE_ADMIN');
INSERT INTO `authorities` VALUES ('ADMIN', 'ROLE_ANALYST');
INSERT INTO `authorities` VALUES ('ADMIN', 'ROLE_MODELER');
INSERT INTO `authorities` VALUES ('ANALYST', 'ROLE_ANALYST');
INSERT INTO `authorities` VALUES ('MODELER', 'ROLE_MODELER');

-- ----------------------------
-- Records of users
-- ----------------------------
INSERT INTO `users` VALUES ('ADMIN', '$2a$10$DuyxBtbmvVi4uQXc5fS.v.H2Uu79/iZYmVSK2K74T/QXjY3vQ2HZq', '1');
INSERT INTO `users` VALUES ('ANALYST', '$2a$10$WUaSh8dNZmad7amA1lUvsOdjHYEJaNY6Ilpq1pfGM2cid4XkbFuCy', '1');
INSERT INTO `users` VALUES ('MODELER', '$2a$10$SETMuht2Wd4yrlDPok4QUuX9DMsmVKXp3PG7/9nRA9DsofVZACX8m', '1');

-- ----------------------------
-- Records of acl_sid
-- ----------------------------
INSERT INTO `acl_sid` VALUES ('1', '1', 'ADMIN');

-- ----------------------------
-- Records of acl_object_identity
-- ----------------------------
INSERT INTO `acl_object_identity` VALUES ('1', '1', 'daa53e80-41be-49a5-90ca-9fb7294db186', null, '1', '0');
INSERT INTO `acl_object_identity` VALUES ('2', '1', '1eaca32a-a33e-4b69-83dd-0bb8b1f8c53b', null, '1', '0');

-- ----------------------------
-- Records of cubes
-- ----------------------------
INSERT INTO `cubes` VALUES ('1eaca32a-a33e-4b69-83dd-0bb8b1f8c53b', 'test_kylin_cube_with_slr', 'ADMIN', null, null, '', null, '2014-04-25', '2014-04-25');
INSERT INTO `cubes` VALUES ('daa53e80-41be-49a5-90ca-9fb7294db186', 'test_kylin_cube_without_slr', 'ADMIN', null, null, null, null, '2014-04-25', '2014-04-25');

-- ----------------------------
-- Records of queries
-- ----------------------------
INSERT INTO `queries` VALUES ('1', 'test_query', 'default','select * from test_table;', 'ADMIN', 'test', '2014-05-04');